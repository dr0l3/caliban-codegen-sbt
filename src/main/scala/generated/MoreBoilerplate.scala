package generated

import Util.{argToWhereClause, doesntReallyMatter, toIntermediateFromReprQuery}
import caliban.InputValue.ObjectValue
import caliban.ResponseValue.RawValue
import caliban.{CalibanError, GraphQL, Http4sAdapter, InputValue, Value}
import caliban.introspection.adt.{__DeprecatedArgs, __Directive, __DirectiveLocation, __Field, __Type, __TypeKind}
import caliban.parsing.adt.{Directive, Type}
import caliban.schema.Step.{MetadataFunctionStep, ObjectStep, PureStep}
import caliban.schema.{Operation, RootSchemaBuilder, Schema, Step, Types}
import generated.Whatever.{Queries, orders, product, users}
import io.circe.{Json, JsonObject}
import zio.{UIO, ZIO}
import caliban.execution.{Field => CField}
import caliban.federation.tracing.ApolloFederatedTracing
import caliban.federation.{EntityResolver, federate}
import caliban.wrappers.Wrapper
import cats.data.Kleisli
import codegen.Runner.{api, query}
import codegen.{ArrayPath, ArrayPathWrapper, Column, ExportedKeyInfo, Intermediate, IntermediateFrom, IntermediateJoin, JsonPathPart, ObejctPathWrapper, ObjectExtension, ObjectExtensionField, ObjectPath, PathWrapper, Table, Util}
import com.sun.xml.internal.bind.v2.TODO
import fs2.Stream
import io.circe.optics.JsonPath.root
import io.circe.optics.{JsonPath, JsonTraversalPath}
import org.http4s.blaze.http.http2.PseudoHeaders.Status
import org.http4s.{EntityBody, Response, StaticFile}
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.implicits.http4sKleisliResponseSyntaxOptionT
import org.http4s.server.Router
import org.http4s.server.middleware.CORS
import zio.query.ZQuery

import java.sql.{Connection, DriverManager}
import java.util.UUID
import scala.concurrent.ExecutionContext


object Util {
  def extractTypeName(tpe: __Type): String = {
    tpe.name match {
      case Some(value) => value
      case None => tpe.ofType match {
        case Some(value) => extractTypeName(value)
        case None => throw new RuntimeException("Should never happen")
      }
    }
  }

  def checkObjectMatches(field: CField, extension: ObjectExtension): Boolean = {

    val typeName = extractTypeName(field.fieldType)

    val res = typeName.contentEquals(extension.objectName) && field.fields.exists(_.name.contentEquals(extension.field.fieldName))
    println(s"CHECK MATCHES OBJECT $typeName = ${extension.objectName} -> $res")
    res
  }

  def includesArray(tpe: __Type): Boolean =  {
    tpe.kind match {
      case __TypeKind.SCALAR => false
      case __TypeKind.OBJECT => false
      case __TypeKind.INTERFACE => false
      case __TypeKind.UNION =>false
      case __TypeKind.ENUM =>false
      case __TypeKind.INPUT_OBJECT =>false
      case __TypeKind.LIST => true
      case __TypeKind.NON_NULL =>
        includesArray(tpe.ofType.getOrElse(throw new RuntimeException("Expected NON_NULL to have inner type")))
    }
  }

  def checkIfExtensionUsed(field: CField, extension: ObjectExtension, path: List[JsonPathPart]): List[List[JsonPathPart]] = {
    println(s"CHECK EXTENSION USED: $extension ${field.fieldType.kind}. ${field.fieldType.name}")
    field.fieldType.kind match {
      case __TypeKind.SCALAR => Nil
      case __TypeKind.OBJECT =>
        println("OBJECT DETECTED")
        if(checkObjectMatches(field, extension)) { // TODO: FIX
          println(s"EXTENSION $extension USED IN OBJECT AT ${field.name}. PATH $path")
          List(path)
        } else {
          field.fields
            .flatMap(field => checkIfExtensionUsed(field, extension, path ++ List(ObjectPath(field.name))))
        }
      case __TypeKind.INTERFACE => Nil
      case __TypeKind.UNION => Nil
      case __TypeKind.ENUM => Nil
      case __TypeKind.INPUT_OBJECT => Nil
      case __TypeKind.LIST =>
        println("LIST DETECTED")
        if(checkObjectMatches(field, extension)) {
          println(s"EXTENSION $extension USED IN LIST AT ${field.name}. PATH $path")
          List(path ++ List(ArrayPath))
        } else {
          println("RECURSING FROM LIST")
          field.fields
            .flatMap(field => checkIfExtensionUsed(field, extension, path ++ List(ArrayPath, ObjectPath(field.name))))
        }
      case __TypeKind.NON_NULL =>
        println("NON NULL DETECTED")
        val includesArray = Util.includesArray(field.fieldType)
        if(checkObjectMatches(field, extension)) {

          println(s"EXTENSION $extension USED IN NON NULL AT ${field.name}. PATH $path")
          if(includesArray) List(path ++ List(ArrayPath))
          else
            List(path)
        } else {
          if(includesArray) {
            field.fields
              .flatMap(field => checkIfExtensionUsed(field, extension, path ++ List(ArrayPath,ObjectPath(field.name))))
          } else
            field.fields
              .flatMap(field => checkIfExtensionUsed(field, extension, path ++ List(ObjectPath(field.name))))
        }
    }

  }

  def createModification(transformer: JsonObject => ZIO[Any,Nothing,JsonObject], path: List[JsonPathPart]): Json => ZIO[Any,Nothing, Json] = {
    import zio.interop.catz._
    path.foldLeft[PathWrapper](ObejctPathWrapper(root)) { (acc, next) =>
      next match {
        case ArrayPath => acc match {
          case ObejctPathWrapper(path) => ArrayPathWrapper(path.each)
          case ArrayPathWrapper(path) => ArrayPathWrapper(path.each)
        }
        case ObjectPath(fieldName) => acc match {
          case ObejctPathWrapper(path) => ObejctPathWrapper(path.selectDynamic(fieldName))
          case ArrayPathWrapper(path) => ArrayPathWrapper(path.selectDynamic(fieldName))
        }
      }
    } match {
      case ObejctPathWrapper(path) => path.obj.modifyF(transformer)
      case ArrayPathWrapper(path) =>path.obj.modifyF(transformer)
    }
  }

  def argToWhereClause(key: String, value: InputValue): String = {
    value match {
      case InputValue.ListValue(values) => "" //TODO
      case InputValue.ObjectValue(fields) => "" //TODO
      case InputValue.VariableValue(name) => "" //TODO
      case value: Value => value match {
        case Value.NullValue => ""
        case value: Value.IntValue => s"$key = ${value}"
        case value: Value.FloatValue => s"$key = $value"
        case Value.StringValue(value) => s"$key = '${value}'"
        case Value.BooleanValue(value) => s"$key = $value"
        case Value.EnumValue(value) => s"$key = '$value''"
      }
    }
  }

  // Generic, straight copy
  def toIntermediateFromReprQuery(field: CField, tables: List[Table], layerName: String, extensions: List[ObjectExtension]): Intermediate = {
    // Top level entity query
    if(field.name.contentEquals("_entities")) {
      val tableName = field.fields.headOption.flatMap(_.parentType).flatMap(_.name).getOrElse(throw new RuntimeException("Unable to find table name for _entities request"))
      val table = tables.find(_.name.contentEquals(tableName.mkString(""))).getOrElse(throw new RuntimeException(s"Unable to find table for ${tableName.mkString("")}"))
      val (queriedCols, recurseCases) = field.fields.partition(_.fields.isEmpty) // TODO: Do dependency analysis
      // SELECT: id, name
      // FROM tablename
      val recursiveCases = recurseCases
        .map(f =>
          toIntermediate(f, tables, s"${layerName}.${f.name}", extensions)
        )
        .map { otherTable =>
          table.relationships
            .find { relationshipTable => relationshipTable.pkTableName == table.name && relationshipTable.fkTableName == otherTable.from.tableName }
            .map(link => IntermediateJoin(link.pkColumnName, link.fkColumnName, otherTable, "TODO"))
            .orElse(table.relationships
              .find { relationshipTable =>
                relationshipTable.fkTableName == table.name && relationshipTable.pkTableName == otherTable.from.tableName
              }
              .map(link => IntermediateJoin(link.fkColumnName, link.pkColumnName, otherTable, "TODO"))
            )
            .getOrElse(throw new RuntimeException(s"Unable to find link between ${table.name} and ${otherTable.from.tableName} required by ${field}"))
        }

      println(s"TABLE $tableName ")
      println(s"COLS: ${queriedCols.map(_.name)}")
      println(s"COL AND DIRS: ${queriedCols.map(col => col.name -> col.directives.map(_.name))}")

      val extensionColumnsForThisTable = extensions.filter(_.objectName.contains(table.name))

      Intermediate(
        queriedCols.filterNot(col => extensionColumnsForThisTable.map(_.field.fieldName).exists(_.contentEquals(col.name))).map(_.name),
        IntermediateFrom(table.name, recursiveCases),
        field
          .copy(fieldType = field.fieldType.copy(kind = __TypeKind.OBJECT)) // TODO: Annoying hack needed to "cast" the output to an object due to calibans weird handling of entityresolvers
      )
    } else {
      toIntermediate(field, tables, layerName, extensions)
    }
  }

  def toIntermediate(field: CField, tables: List[Table], layerName: String, extensions: List[ObjectExtension]): Intermediate = {
    println(tables)
    val tableName = field.fieldType.ofType match {
      case Some(value) => compileOfType(value)
      case None => List(field.fieldType.name.getOrElse(throw new RuntimeException(s"No typename for field ${field}")))
    }
    val table = tables.find(_.name.contentEquals(tableName.mkString(""))).getOrElse(throw new RuntimeException(s"Unable to find table for ${tableName.mkString("")}"))
    val (queriedCols, recurseCases) = field.fields.partition(_.fields.isEmpty) // TODO: Do dependency analysis
    // SELECT: id, name
    // FROM tablename
    val recursiveCases = recurseCases
      .map(f =>
        toIntermediate(f, tables, s"${layerName}.${f.name}", extensions)
      )
      .map { otherTable =>
        table.relationships
          .find { relationshipTable => relationshipTable.pkTableName == table.name && relationshipTable.fkTableName == otherTable.from.tableName }
          .map(link => IntermediateJoin(link.pkColumnName, link.fkColumnName, otherTable, "TODO"))
          .orElse(table.relationships
            .find { relationshipTable =>
              relationshipTable.fkTableName == table.name && relationshipTable.pkTableName == otherTable.from.tableName
            }
            .map(link => IntermediateJoin(link.fkColumnName, link.pkColumnName, otherTable, "TODO"))
          )
          .getOrElse(throw new RuntimeException(s"Unable to find link between ${table.name} and ${otherTable.from.tableName} required by ${field}"))
      }

    println(s"TABLE $tableName ")
    println(s"COLS: ${queriedCols.map(_.name)}")
    println(s"COL AND DIRS: ${queriedCols.map(col => col.name -> col.directives.map(_.name))}")

    val extensionColumnsForThisTable = extensions.filter(_.objectName.contains(table.name))

    Intermediate(queriedCols.filterNot(col => extensionColumnsForThisTable.map(_.field.fieldName).exists(_.contentEquals(col.name))).map(_.name), IntermediateFrom(table.name, recursiveCases), field)
  }

  def compileFrom(intermediate: Intermediate, top: Boolean): String = {
    val joins = intermediate.from.joins.map { join =>
      s""" LEFT JOIN ${join.right.from.tableName} on ${intermediate.from.tableName}.${join.leftColName} = ${join.right.from.tableName}.${join.rightColName} \n""" ++ compileFrom(join.right, top = false)
    }.mkString("")

    if (top) {
      intermediate.from.tableName ++ joins
    } else {
      joins
    }
  }

  def compileCols(intermediate: Intermediate): List[String] = {
    intermediate.cols.map(col => s"${intermediate.from.tableName}.${col}") ++ intermediate.from.joins.map(_.right).flatMap(compileCols)
  }

  def toQueryNaive(intermediate: Intermediate): String = {
    val allCols = compileCols(intermediate)

    val from = compileFrom(intermediate, top = true)

    s"SELECT ${allCols.mkString(",")} FROM $from"
  }

  /*

  Both Hasura and Postgraphile renders the json in the database.

  When you see the queries that sort of makes sense. The problem with getting a "regular" resultset back is that parsing the relationships becomes difficult.
  For example if you have a simple query like

  'select a,b,c from A leftjoin B leftjoin C'

  Then if the relationship is one to many both between A and B and B and C then
  you will have many many duplicated values of a around. If there is 1 A row,
  and for each A-row there is 2 B-rows and for each B-row there is 2 C-rows. then we get the following.

  a1 b1 c1
  a1 b1 c2
  a1 b2 c3
  a1 b2 c4

  The duplication quickly gets a little out of hand with very nested GraphQL queries.

  When the json is rendered in the database the data starts to look like it would in a regular application.
  For each table the data is fetched, its rendered into json form and then spliced into the outer query.
  At the end this is then returned to the client.


   */

  /*

  Output

  select row_to_json(select col1 as fieldName1, col2 as fieldName2)
  from (select * from table where ???)
    left outer join lateral (
      select col3 as fieldName3, col4 as fieldName4
      from (select * from table2 where ???))

   Algorithm

   DFS on the tree

   The aliases involved
   - alias for the data
   - alias for json formatted data
   - alias for the name of the field the data is stored under (root for rootnode)

   */

  //
  def toQueryWithJson(intermediate: Intermediate, fieldNameOfObject: String, condition: String): String = {
    val dataId = s"${fieldNameOfObject}_data"
    val cols = intermediate.cols.map { col =>
      s""""$dataId"."$col""""
    } ++ intermediate.from.joins.map { join =>

      val fieldName = join.right.field.name
      s""" "$fieldNameOfObject.$fieldName"."$fieldNameOfObject.$fieldName" as $fieldName """
    }

    val thisTable =
      s"""
         |select row_to_json(
         |    (
         |      select "json_spec"
         |      from (
         |        select ${cols.mkString(",")}
         |      ) as "json_spec"
         |     )
         |) as "$fieldNameOfObject"
         |from (select * from ${intermediate.from.tableName} where $condition) as "$dataId"
         |    """.stripMargin

    val joins = intermediate.from.joins.map { join =>
      (
        toQueryWithJson(join.right, s"""${fieldNameOfObject}.${join.right.field.name}""", s""""$dataId"."${join.leftColName}" = "${join.rightColName}""""),
        s"${fieldNameOfObject}.${join.right.from.tableName}"
      )
    }

    val complete = joins
      .foldLeft(thisTable)((acc, next) => {
        acc ++ s""" LEFT JOIN LATERAL (${next._1}) as "${next._2}" on true """
      })

    intermediate.field.fieldType.kind match {
      case __TypeKind.OBJECT =>

        s"""|SELECT coalesce((json_agg("root") -> 0), 'null') AS "$fieldNameOfObject"
            |from ($complete) as "$fieldNameOfObject" """.stripMargin

      // List[A] and Option[List[A]] gets treated the same. No rows -> Empty list
      case __TypeKind.LIST =>
        s"""
           |select coalesce(json_agg("$fieldNameOfObject"), '[]') as "${fieldNameOfObject}"
           |from ($complete) as "$fieldNameOfObject"
           |""".stripMargin
      // List[A] and Option[List[A]] gets treated the same. No rows -> Empty list
      case __TypeKind.NON_NULL if intermediate.field.fieldType.ofType.map(_.kind).contains(__TypeKind.LIST) =>

        s"""
           |select coalesce(json_agg("$fieldNameOfObject"), '[]') as "${fieldNameOfObject}"
           |from ($complete) as "$fieldNameOfObject"
           |""".stripMargin

      case __TypeKind.NON_NULL =>
        complete
      // Below is irrelevant
      case __TypeKind.SCALAR => ""
      case __TypeKind.INTERFACE => ""
      case __TypeKind.UNION => ""
      case __TypeKind.ENUM => ""
      case __TypeKind.INPUT_OBJECT => ""
    }
  }

  def compileOfType(tpe: __Type): List[String] = {
    tpe.ofType match {
      case Some(value) => tpe.name.toList ++ compileOfType(value)
      case None => tpe.name.toList
    }
  }

  def doesntReallyMatter(field: CField, extensions: Map[ObjectExtension, JsonObject => ZIO[Any,Nothing,JsonObject]]): List[Json => ZIO[Any, Nothing,Json]] = {
    extensions.flatMap { case (extension, transformer) =>
      checkIfExtensionUsed(field, extension, Nil).map(path => path -> transformer)
    }.map { case (path, transformer) =>
      println(s"PATH: $path has transformer")
      createModification(transformer, path)
    }.toList
  }

  def typeTo__Type(tpe: Type): __Type = {
    tpe match {
      case Type.NamedType(name, nonNull) =>
        val inner = name match {
          case "String" => Types.string
          case "String!" => Types.string
          case "Int" => Types.int
          case "Int!" => Types.int
          case "Boolean" => Types.boolean
          case "Boolean!" => Types.boolean
          case "Long" => Types.long
          case "Long!" => Types.long
          case "Float" => Types.float
          case "Float!" => Types.float
          case "Double" => Types.double
          case "Double!" => Types.double
        }
        if(nonNull) Types.makeNonNull(inner) else inner
      case Type.ListType(ofType, nonNull) => Types.makeList(typeTo__Type(ofType))
    }
  }

  def objectExtensionFieldToField(extension: ObjectExtensionField): __Field = {
    __Field(extension.fieldName, None, Nil, () => typeTo__Type(extension.fieldType), false, None, Option(Nil)) //TODO: We would like to  Caliban does not write out custom directives as part of the schema. This is not spec compliant so the Apollo gateway will complain.
  }

  def checkExtensionApplies(tpe: __Type, extension :ObjectExtension): Boolean =  {

    val name = Util.extractTypeName(tpe)

    if(name.contentEquals(extension.objectName)) {
      true
    } else {
      false
    }

  }

  def extendType(tpe: __Type, extensions: List[ObjectExtension]): __Type = {
    tpe.kind match {
      case __TypeKind.SCALAR | __TypeKind.INTERFACE | __TypeKind.UNION | __TypeKind.ENUM  | __TypeKind.INPUT_OBJECT => tpe
      case __TypeKind.OBJECT =>
        val extensionsToApply = extensions.filter(extension => checkExtensionApplies(tpe, extension)).map(_.field)
        extensionsToApply match {
          case Nil =>
            def computeFields(a: __DeprecatedArgs): Option[List[__Field]] = {
              val patchedFields = tpe.fields(a).getOrElse(Nil).map { field =>
                field.copy(`type` = () => extendType(field.`type`(), extensions))
              }
              Option(patchedFields)
            }
            tpe.copy(fields = computeFields)
          case list =>
            def computeFields(a: __DeprecatedArgs): Option[List[__Field]] = {
              val patchedFields = tpe.fields(a).getOrElse(Nil).map { field =>
                field.copy(`type` = () => extendType(field.`type`(), extensions))
              }

              Option(patchedFields ++ list.map(objectExtensionFieldToField))
            }
            tpe.copy(fields = computeFields)
        }
      case __TypeKind.LIST | __TypeKind.NON_NULL  =>
        tpe.copy(ofType = tpe.ofType.map(extendType(_, extensions)))
    }
  }
}

object ApiCreator {

  val tables = List(Table("lineitems", List(), List(Column("orderid", false, 1111, "uuid", false),Column("product_id", false, 12, "text", false),Column("amount", false, 4, "int4", false)), "null", List(ExportedKeyInfo("orders", "id", "lineitems", "orderid", 1, Some("orders_pkey"), Some("lineitems_orderid_fkey")),ExportedKeyInfo("product", "upc", "lineitems", "product_id", 1, Some("product_pkey"), Some("lineitems_product_id_fkey")))),Table("orders", List(Column("id", false, 1111, "uuid", true)), List(Column("buyer", false, 1111, "uuid", false),Column("ordered_at", false, 93, "timestamptz", false)), "null", List(ExportedKeyInfo("orders", "id", "lineitems", "orderid", 1, Some("orders_pkey"), Some("lineitems_orderid_fkey")),ExportedKeyInfo("users", "id", "orders", "buyer", 1, Some("users_pkey"), Some("orders_buyer_fkey")))),Table("product", List(Column("upc", false, 12, "text", true)), List(Column("name", true, 12, "text", false),Column("price", true, 4, "int4", false),Column("weight", true, 4, "int4", false)), "null", List(ExportedKeyInfo("product", "upc", "lineitems", "product_id", 1, Some("product_pkey"), Some("lineitems_product_id_fkey")),ExportedKeyInfo("product", "upc", "review", "product", 1, Some("product_pkey"), Some("review_product_fkey")))),Table("review", List(), List(Column("author", false, 1111, "uuid", false),Column("product", false, 12, "text", false),Column("body", true, 12, "text", false)), "null", List(ExportedKeyInfo("product", "upc", "review", "product", 1, Some("product_pkey"), Some("review_product_fkey")),ExportedKeyInfo("users", "id", "review", "author", 1, Some("users_pkey"), Some("review_author_fkey")))),Table("users", List(Column("id", false, 1111, "uuid", true)), List(Column("name", true, 12, "text", false),Column("age", true, 4, "int4", false),Column("email", true, 12, "text", true)), "null", List(ExportedKeyInfo("users", "id", "orders", "buyer", 1, Some("users_pkey"), Some("orders_buyer_fkey")),ExportedKeyInfo("users", "id", "review", "author", 1, Some("users_pkey"), Some("review_author_fkey")))))

  def ordersReprToCondition(input: ObjectValue): String = {
    ("id" ++ " = '" ++ input.fields("id").toString.replaceAllLiterally("\"", "") ++ "'" )
  }


  def productReprToCondition(input: ObjectValue): String = {
    ("upc" ++ " = '" ++ input.fields("upc").toString.replaceAllLiterally("\"", "") ++ "'" )
  }


  def usersReprToCondition(input: ObjectValue): String = {
    ("id" ++ " = '" ++ input.fields("id").toString.replaceAllLiterally("\"", "") ++ "'" )
  }



  val tpeToConditionHandler: Map[String, ObjectValue => String] = Map(
    "orders" -> ordersReprToCondition,
    "product" -> productReprToCondition,
    "users" -> usersReprToCondition
  )



  def createFederatedApi[R](
                             connection: Connection,
                             usersaddressExtension: (UUID) => ZIO[Any,Nothing,String]
                           )(implicit querySchema: Schema[R, Queries], ordersSchema: Schema[R, orders], productSchema: Schema[R, product], usersSchema: Schema[R, users]): GraphQL[R] = {
    val original = createApi(Nil, connection, usersaddressExtension)


    def usersaddressExtensionConverter(jsonObject: JsonObject): ZIO[Any,Nothing,JsonObject] = {
      usersaddressExtension(jsonObject.apply("id").get.as[UUID].right.get).map(output => jsonObject.add("address", Json.fromString(output)))
    }


    val extensionLogicByType: Map[ObjectExtension, JsonObject => ZIO[Any,Nothing,JsonObject]] = Map(
      ObjectExtension("users", ObjectExtensionField("address", Type.NamedType("String!", nonNull = true), Nil)) -> usersaddressExtensionConverter
    )



    def genericHandler(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
      ZQuery.succeed(Step.MetadataFunctionStep(field => {

        val intermediate = toIntermediateFromReprQuery(field, tables, "_entities", extensionLogicByType.keys.toList)

        val otherArg = {
          value match {
            case InputValue.ListValue(values) => ""
            case o @ ObjectValue(fields) =>
              tpeToConditionHandler(fields("__typename").toInputString.replaceAllLiterally("\"", ""))(o)
            case InputValue.VariableValue(name) => ""
            case value: Value => ""
          }
        }
        val condition = otherArg
        val query = Util.toQueryWithJson(intermediate, "root", condition)
        val prepared = connection.prepareStatement(query)

        println(query)
        val rs = prepared.executeQuery()
        rs.next()
        val resultJson = rs.getString("root")
        println(resultJson)

        val json = io.circe.parser.parse(resultJson).getOrElse(Json.Null)

        val modificaitons = doesntReallyMatter(field, extensionLogicByType)

        val modifiedJson = modificaitons.foldLeft(ZIO.succeed(json)){ (acc, next) =>
          acc.flatMap(next(_))
        }

        val jsonRaw = zio.Runtime.global.unsafeRun(modifiedJson).noSpaces


        PureStep(RawValue(jsonRaw)) // TODO: FIx

      }))
    }


    val ordersEntityResolver = new EntityResolver[R] {
      override def resolve(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
        genericHandler(value)
      }

      override def toType: __Type = Util.extendType(ordersSchema.toType_(), extensionLogicByType.keys.toList)
    }


    val productEntityResolver = new EntityResolver[R] {
      override def resolve(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
        genericHandler(value)
      }

      override def toType: __Type = Util.extendType(productSchema.toType_(), extensionLogicByType.keys.toList)
    }


    val usersEntityResolver = new EntityResolver[R] {
      override def resolve(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
        genericHandler(value)
      }

      override def toType: __Type = Util.extendType(usersSchema.toType_(), extensionLogicByType.keys.toList)
    }


    val entityResolverList = List(productEntityResolver,usersEntityResolver)

    federate(
      original,
      ordersEntityResolver,
      entityResolverList: _*
    )
  }

  def createApi[R](
                    directives: List[__Directive] = Nil,
                    connection: Connection,
                    usersaddressExtension: (UUID) => ZIO[Any,Nothing,String]
                  )(
                    implicit
                    querySchema: Schema[R, Queries]
                  ): GraphQL[R] = new GraphQL[R] {


    def constructOperation[R,Q](schema: Schema[R,Q])(implicit connection: Connection): Operation[R] = {
      def usersaddressExtensionConverter(jsonObject: JsonObject): ZIO[Any,Nothing,JsonObject] = {
        usersaddressExtension(jsonObject.apply("id").get.as[UUID].right.get).map(output => jsonObject.add("address", Json.fromString(output)))
      }


      val extensionLogicByType: Map[ObjectExtension, JsonObject => ZIO[Any,Nothing,JsonObject]] = Map(
        ObjectExtension("users", ObjectExtensionField("address", Type.NamedType("String!", nonNull = true), Nil)) -> usersaddressExtensionConverter
      )


      val tpe = Util.extendType(schema.toType_(), extensionLogicByType.keys.toList)

      def createFieldHandlers(a: __DeprecatedArgs): Map[String, MetadataFunctionStep[R]] = {
        val rootFields = tpe.fields(a).getOrElse(Nil)

        rootFields.map { rootField =>
          def handler(field: CField): Step[R] = {
            // TODO: Possibly do some sort of requirement analysis
            // TODO: Create field mask
            // TODO: Add additional required fields

            // We have created an intermediate that has filtered out all the fields that have @requires
            val intermediate = Util.toIntermediate(field, tables, "root", extensionLogicByType.keys.toList)
            val fieldMask: List[Json => Json] = Nil // Transformations for removing the fields that were added to satisfy extensions

            val condition = field.arguments.map { case (k, v) => argToWhereClause(k, v) }.mkString(" AND ")
            val query = Util.toQueryWithJson(intermediate, "root", condition)
            val prepared = connection.prepareStatement(query)

            println(query)
            val rs = prepared.executeQuery()
            rs.next()
            val resultJson = rs.getString("root")
            println(resultJson)

            val json = io.circe.parser.parse(resultJson).getOrElse(Json.Null)

            val modificaitons = doesntReallyMatter(field, extensionLogicByType)

            val modifiedJson = modificaitons.foldLeft(ZIO.succeed(json)){ (acc, next) =>
              acc.flatMap(next(_))
            }

            PureStep(RawValue(zio.Runtime.global.unsafeRun(modifiedJson).spaces4)) // TODO: FIx
          }

          rootField.name -> MetadataFunctionStep(handler)
        }.toMap
      }

      val queryFields = createFieldHandlers(__DeprecatedArgs(Option(true)))
      val resolver = ObjectStep(
        tpe.name.getOrElse("queries"),
        queryFields
      )
      Operation(tpe, resolver)
    }

    override protected val schemaBuilder: RootSchemaBuilder[R] = RootSchemaBuilder(
      Option(
        constructOperation(querySchema)(connection)
      ),
      None,
      None
    )
    override protected val wrappers: List[Wrapper[R]] = Nil
    override protected val additionalDirectives: List[__Directive] = directives
  }
}

object ApiCreator2 {
  val tables = List(
    Table("lineitems", List(), List(Column("orderid", false, 1111, "uuid", false), Column("product_id", false, 12, "text", false), Column("amount", false, 4, "int4", false)), "null", List(ExportedKeyInfo("orders", "id", "lineitems", "orderid", 1, Some("orders_pkey"), Some("lineitems_orderid_fkey")), ExportedKeyInfo("product", "upc", "lineitems", "product_id", 1, Some("product_pkey"), Some("lineitems_product_id_fkey")))),
    Table("orders", List(Column("id", false, 1111, "uuid", true)), List(Column("buyer", false, 1111, "uuid", false), Column("ordered_at", false, 93, "timestamptz", false)), "null", List(ExportedKeyInfo("orders", "id", "lineitems", "orderid", 1, Some("orders_pkey"), Some("lineitems_orderid_fkey")), ExportedKeyInfo("users", "id", "orders", "buyer", 1, Some("users_pkey"), Some("orders_buyer_fkey")))),
    Table("product", List(Column("upc", false, 12, "text", true)), List(Column("name", true, 12, "text", false), Column("price", true, 4, "int4", false), Column("weight", true, 4, "int4", false)), "null", List(ExportedKeyInfo("product", "upc", "lineitems", "product_id", 1, Some("product_pkey"), Some("lineitems_product_id_fkey")), ExportedKeyInfo("product", "upc", "review", "product", 1, Some("product_pkey"), Some("review_product_fkey")))),
    Table("review", List(), List(Column("author", false, 1111, "uuid", false), Column("product", false, 12, "text", false), Column("body", true, 12, "text", false)), "null", List(ExportedKeyInfo("product", "upc", "review", "product", 1, Some("product_pkey"), Some("review_product_fkey")), ExportedKeyInfo("users", "id", "review", "author", 1, Some("users_pkey"), Some("review_author_fkey")))),
    Table("users", List(Column("id", false, 1111, "uuid", true)), List(Column("name", true, 12, "text", false), Column("age", true, 4, "int4", false), Column("email", true, 12, "text", true)), "null", List(ExportedKeyInfo("users", "id", "orders", "buyer", 1, Some("users_pkey"), Some("orders_buyer_fkey")), ExportedKeyInfo("users", "id", "review", "author", 1, Some("users_pkey"), Some("review_author_fkey"))))
  )

  val customDirective = List(__Directive("customextension", None, Set(__DirectiveLocation.FIELD), Nil))

  // Need to codegen this for each table
  def orderReprToCondition(input: ObjectValue): String = {
    s"id = '${input.fields("id").toString.replaceAllLiterally("\"", "")}'"
  }

  // And this as the aggregator
  val tpeToWhateverHandlers: Map[String, ObjectValue => String] = Map(
    "orders" -> orderReprToCondition
  )

  val idFieldLookupMap= Map(
    "orders" -> "id"
  )

  def createFederatedApi[R](
    connection: Connection,
    userAddressExtesnions: UUID => ZIO[Any,Nothing, String]
                           )(implicit querySchema: Schema[R, Queries], orderSchema: Schema[R, orders]): GraphQL[R] = {
    val original = createApi(customDirective, connection, userAddressExtesnions)

    def usersaddressExtensionConverter(jsonObject: JsonObject): ZIO[Any,Nothing,JsonObject] = {
      userAddressExtesnions(jsonObject.apply("id").get.as[UUID].right.get).map(output => jsonObject.add("address", Json.fromString(output)))
    }

    val extensionLogicByType: Map[ObjectExtension, JsonObject => ZIO[Any,Nothing,JsonObject]] = Map(
      ObjectExtension("users", ObjectExtensionField("address", Type.NamedType("String!", nonNull = true), Nil)) -> usersaddressExtensionConverter
    )

    def genericHandler(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
      println(s"RESOVLECALLED: ${value.toInputString}")
      ZQuery.succeed(Step.MetadataFunctionStep(field => {

        val intermediate = toIntermediateFromReprQuery(field, tables, "_entities", extensionLogicByType.keys.toList)
        println(field.arguments("representations"))
        // [{"__typename:"orders"","id:"ebdc67f6-8925-4d98-948d-eb5042dc63fc""}]
        println(field.arguments("representations").toInputString)
        // [{__typename: "orders", id: "ebdc67f6-8925-4d98-948d-eb5042dc63fc"}]
        val zomg = field.arguments("representations").toInputString.replace("(\\\"(.*?)\\\"|(\\w+))(\\s*:\\s*(\\\".*?\\\"|.))", "\"$2$3\"$4")
        println(zomg)

        val otherArg = {
          value match {
            case InputValue.ListValue(values) => ""
            case o @ ObjectValue(fields) =>
              tpeToWhateverHandlers(fields("__typename").toInputString.replaceAllLiterally("\"", ""))(o)
            case InputValue.VariableValue(name) => ""
            case value: Value => ""
          }
        }


        // The federation implementation insists on processing each of entityresolver args alone instead of in batch. This means that whatever is returned here is wrapped in a list.
        // This is the "correct" way of handling this would be to just allow me to return the list using one query, but I guess that will require some modifications to Calibans federation handling.
        /*
        val args = field.arguments("representations") match {
          case InputValue.ListValue(values) => values.map {
            case InputValue.ListValue(values) => ""
            case o @ ObjectValue(fields) =>
              val tpe = fields("__typename").toInputString.replaceAllLiterally("\"", "")
              println(s"TPE: $tpe")
              tpeToWhateverHandlers(tpe)(o)
            case InputValue.VariableValue(name) => ""
            case value: Value => ""
          }.mkString(" OR ")
          case ObjectValue(fields) => ""
          case InputValue.VariableValue(name) => ""
          case value: Value => ""
        }*/


        val condition = otherArg
        val query = Util.toQueryWithJson(intermediate, "root", condition)
        val prepared = connection.prepareStatement(query)

        println(query)
        val rs = prepared.executeQuery()
        rs.next()
        val resultJson = rs.getString("root")
        println(resultJson)

        val json = io.circe.parser.parse(resultJson).getOrElse(Json.Null)

        val modificaitons = doesntReallyMatter(field, extensionLogicByType)

        val modifiedJson = modificaitons.foldLeft(ZIO.succeed(json)){ (acc, next) =>
          acc.flatMap(next(_))
        }

        val jsonRaw = zio.Runtime.global.unsafeRun(modifiedJson).noSpaces

        println(jsonRaw)

        PureStep(RawValue(jsonRaw)) // TODO: FIx

      }))
    }

    val ordersEntityResolver = new EntityResolver[R] {
      override def resolve(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
        genericHandler(value)
      }

      override def toType: __Type = orderSchema.toType_()
    }

    federate(
      original,
      ordersEntityResolver
    )
  }

  def createApi[R](
                    directives: List[__Directive] = List(__Directive("customextension", None, Set(__DirectiveLocation.FIELD), Nil)),
                    connection: Connection,
                    usersaddressExtension: (UUID) => ZIO[Any,Nothing,String]
                  )(
                    implicit
                    querySchema: Schema[R, Queries]
                  ): GraphQL[R] = new GraphQL[R] {




    def constructOperation[R,Q](schema: Schema[R,Q])(implicit connection: Connection): Operation[R] = {
      def usersaddressExtensionConverter(jsonObject: JsonObject): ZIO[Any,Nothing,JsonObject] = {
        usersaddressExtension(jsonObject.apply("id").get.as[UUID].right.get).map(output => jsonObject.add("address", Json.fromString(output)))
      }

      val extensionLogicByType: Map[ObjectExtension, JsonObject => ZIO[Any,Nothing,JsonObject]] = Map(
        ObjectExtension("users", ObjectExtensionField("address", Type.NamedType("String!", nonNull = true), Nil)) -> usersaddressExtensionConverter
      )
      val tpe = Util.extendType(schema.toType_(), extensionLogicByType.keys.toList)

      def createFieldHandlers(a: __DeprecatedArgs): Map[String, MetadataFunctionStep[R]] = {
        val rootFields = tpe.fields(a).getOrElse(Nil)

        rootFields.map { rootField =>
          def handler(field: CField): Step[R] = {
            // TODO: Possibly do some sort of requirement analysis
            // TODO: Create field mask
            // TODO: Add additional required fields

            // We have created an intermediate that has filtered out all the fields that have @requires
            val intermediate = Util.toIntermediate(field, tables, "root", extensionLogicByType.keys.toList)
            val fieldMask: List[Json => Json] = Nil // Transformations for removing the fields that were added to satisfy extensions

            val condition = field.arguments.map { case (k, v) => argToWhereClause(k, v) }.mkString(" AND ")
            val query = Util.toQueryWithJson(intermediate, "root", condition)
            val prepared = connection.prepareStatement(query)

            println(query)
            val rs = prepared.executeQuery()
            rs.next()
            val resultJson = rs.getString("root")
            println(resultJson)

            val json = io.circe.parser.parse(resultJson).getOrElse(Json.Null)

            val modificaitons = doesntReallyMatter(field, extensionLogicByType)

            val modifiedJson = modificaitons.foldLeft(ZIO.succeed(json)){ (acc, next) =>
              acc.flatMap(next(_))
            }

            PureStep(RawValue(zio.Runtime.global.unsafeRun(modifiedJson).spaces4)) // TODO: FIx
          }

          rootField.name -> MetadataFunctionStep(handler)
        }.toMap
      }

      val queryFields = createFieldHandlers(__DeprecatedArgs(Option(true)))
      val resolver = ObjectStep(
        tpe.name.getOrElse("queries"),
        queryFields
      )
      Operation(tpe, resolver)
    }

    override protected val schemaBuilder: RootSchemaBuilder[R] = RootSchemaBuilder(
      Option(
        constructOperation(querySchema)(connection)
      ),
      None,
      None
    )
    override protected val wrappers: List[Wrapper[R]] = Nil
    override protected val additionalDirectives: List[__Directive] = directives
  }
}


import zio._

object SuperApp extends App {
  import codegen.PostgresSniffer.connToTables
  val _ = classOf[org.postgresql.Driver]
  val con_str = "jdbc:postgresql://0.0.0.0:5438/product"
  implicit val conn = DriverManager.getConnection(con_str, "postgres", "postgres")
  import generated.Whatever._

  val api = ApiCreator.createFederatedApi(conn, id => UIO(s"WOOO: $id"))

  val query =
    """
      |{
      |  get_orders_by_id(id:"ebdc67f6-8925-4d98-948d-eb5042dc63fc") {
      |    id
      |    buyer
      |    ordered_at
      |    users {
      |      id
      |      name
      |      age
      |      email
      |      address
      |    }
      |    lineitems {
      |      product_id
      |      amount
      |      product {
      |        upc
      |        name
      |        price
      |        weight
      |        review {
      |          author
      |          users {
      |            id
      |            name
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
      |""".stripMargin


  api.interpreter

//  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
//    (for {
//
//      interpreter <- api.interpreter
//      res <- interpreter.execute(query)
//      _ = println(io.circe.parser.parse(res.data.toString).right.get.spaces4)
//      //    _ = println(api.render)
//    } yield ()).exitCode
//  }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    import zio.interop.catz._

    type Whatever[A] = RIO[ZEnv, A]

    (for {
      interpreter <- api.@@(ApolloFederatedTracing.wrapper).interpreter
      exit <- BlazeServerBuilder[Whatever](ExecutionContext.global).bindHttp(8088, "localhost")
        .withHttpApp(
          Router[Whatever](
            "/graphql" -> CORS(Http4sAdapter.makeHttpService(interpreter)),
            "/graphiql"    -> Kleisli.liftF(StaticFile.fromResource("/graphiql.html", None))
          ).orNotFound
        )
        .resource
        .toManagedZIO
        .useForever
        .exitCode
    } yield exit).exitCode
  }
}