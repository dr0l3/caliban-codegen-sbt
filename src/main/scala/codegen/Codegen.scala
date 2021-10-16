package codegen

import caliban.ResponseValue.RawValue
import caliban._
import caliban.execution.{Field => CField}
import caliban.federation.{EntityResolver, federate}
import caliban.introspection.adt.{__DeprecatedArgs, __Directive, __Field, __Type, __TypeKind}
import caliban.parsing.adt.Definition.TypeSystemDefinition
import caliban.parsing.adt.Definition.TypeSystemDefinition.TypeDefinition
import caliban.parsing.adt.Definition.TypeSystemDefinition.TypeDefinition.ObjectTypeDefinition
import caliban.parsing.adt.{Definition, Directive, Type}
import caliban.parsing.parsers.Parsers.document
import caliban.parsing.{ParsedDocument, SourceMapper}
import caliban.schema.Step.{MetadataFunctionStep, ObjectStep, PureStep}
import caliban.schema.{Operation, RootSchemaBuilder, Schema, Step, Types}
import caliban.wrappers.Wrapper
//import codegen.Runner.api
import fastparse.parse
import generated.Whatever.{orders, users}
import io.circe.optics.JsonPath.root
import io.circe.optics.{JsonPath, JsonTraversalPath}
import io.circe.{Json, JsonObject, parser}
import zio.query.ZQuery
import zio.{UIO, ZIO}

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.UUID
import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.io.File


sealed trait JsonPathPart
case object ArrayPath extends JsonPathPart
case class ObjectPath(fieldName: String) extends JsonPathPart


case class ObjectExtensionField(fieldName: String, fieldType: Type, requiredFields: List[String])
case class ObjectExtension(objectName: String, field: ObjectExtensionField)


// Wrapper neccesary for fold the fold left to work
sealed trait PathWrapper
case class ObejctPathWrapper(path: JsonPath) extends PathWrapper
case class ArrayPathWrapper(path: JsonTraversalPath) extends PathWrapper

sealed trait JsonSelection
case class ObjectFieldSelection(fieldName: String) extends JsonSelection
case object ArraySelection extends JsonSelection

case class Join(tableName: String, leftCol: String, rightCol: String)


case class From(tableName: String, join: List[Join])

case class SQLQuery(cols: List[String], from: From)

case class IntermediateJoin(leftColName: String, rightColName: String, right: Intermediate, name: String)

case class IntermediateFrom(tableName: String, joins: List[IntermediateJoin])

case class Intermediate(cols: List[String], from: IntermediateFrom, field: CField)

case class FunctionInfo(name: String, comments: String, functionType: Short)

case class FunctionColumnInfo(functionName: String, columnName: String, columnType: Short, dataType: Int, typeName: String, nullable: Boolean, comments: String)

case class ProcedureColumnInfo(procedureName: String, columnName: String, columnType: Short, dataType: Int, nullable: Boolean)

case class ProcedureInfo(name: String, comments: String)

case class PrimaryKeyInfo(columnName: String, keySeq: Int, tableName: String, pkName: String)

case class TableInfo(name: String, comment: String)

case class ColumnInfo(name: String, nullable: Boolean, sqltype: Int, typeName: String, tableName: String)

case class IndexInfo(tableName: String, indexName: String, indexType: Int, columnName: String, nonUnique: Boolean)

case class ExportedKeyInfo(pkTableName: String, pkColumnName: String, fkTableName: String, fkColumnName: String, keySeq: Int, pkName: Option[String], fkName: Option[String]) {
  def renderSelf(): String = {
    val renderedPkName = pkName.map(name =>s"""Some("${name}")""").getOrElse("None")
    val renderedFkName = fkName.map(name=> s"""Some("$name")""").getOrElse("None")
    s"""ExportedKeyInfo("$pkTableName", "$pkColumnName", "$fkTableName", "$fkColumnName", $keySeq, $renderedPkName, $renderedFkName)"""
  }
}

case class ImportedKeyInfo(pkTableName: String, pkColumnName: String, fkTableName: String, fkColumnName: String, keySeq: Int, pkName: Option[String], fkName: Option[String])

case class Column(name: String, nullable: Boolean, sqltype: Int, typeName: String, unique: Boolean) {
  def renderSelf(): String = {
    s"""Column("$name", $nullable, $sqltype, "$typeName", $unique)"""
  }
}


case class Table(name: String, primaryKeys: List[Column], otherColumns: List[Column], comments: String, relationships: List[ExportedKeyInfo] = Nil) {
  def renderSelf(): String = {
    val renderedPrimaryKeys = primaryKeys.map(_.renderSelf()).mkString("List(", ",", ")")
    val renderedOtherColumns = otherColumns.map(_.renderSelf()).mkString("List(",",", ")")
    val renderedRelationships = relationships.map(_.renderSelf()).mkString("List(", ",", ")")
    s"""Table("${name}", $renderedPrimaryKeys, $renderedOtherColumns, "$comments", $renderedRelationships)"""
  }
}


sealed trait SQLType

case object Integer extends SQLType

sealed trait ScalaType {
  def render: String

  def name: String
}

sealed trait BaseType extends ScalaType

case object UUIDType extends BaseType {
  override def render: String = "UUID"

  override def name: String = "UUID"
}

case object IntegerType extends BaseType {
  override def render: String = "Int"

  override def name: String = "Int"
}

case object StringType extends BaseType {
  override def render: String = "String"

  override def name: String = render
}

case object ZonedDateTimeType extends BaseType {
  override def render: String = "ZonedDateTime"

  override def name: String = render
}

sealed trait ContainerType extends ScalaType

case class ListType(elementType: ScalaType) extends ContainerType {
  override def render: String = s"List[${elementType.name}]"

  override def name: String = render
}

case class OptionType(elementType: ScalaType) extends ContainerType {
  override def render: String = s"Option[${elementType.name}]"

  override def name: String = render
}

sealed trait FieldType {
  def toName(): String
}

case class Concrete(scalaType: ScalaType) extends FieldType {
  override def toName(): String = scalaType.name
}

case class Reference(scalaType: () => ScalaType) extends FieldType {
  override def toName(): String = scalaType().name
}

case class Field(name: String, scalaType: FieldType, isPrimaryKey: Boolean, isExtension: Boolean, requires: List[String])

case class CaseClassType(name: String, fields: List[Field]) extends ScalaType {
  def render = {
    val fieldRendering = fields.map { field =>
      val typeRendering = field.scalaType match {
        case Concrete(scalaType) => scalaType.name
        case Reference(scalaType) => scalaType().name
      }
      s"${field.name}: ${typeRendering}"
    }
    s"case class ${name}(${fieldRendering.mkString(",")})"
  }

  def renderWithFederation() = {
    val fieldRendering = fields.map { field =>
      val typeRendering = field.scalaType match {
        case Concrete(scalaType) => scalaType.name
        case Reference(scalaType) => scalaType().name
      }
      s"${field.name}: ${typeRendering}"
    }

    val keys = fields.filter(_.isPrimaryKey).map(_.name)

    if(keys.nonEmpty) {
      s"""@GQLDirective(Key("${keys.mkString(" ")}")) case class ${name}(${fieldRendering.mkString(",")})"""
    } else {
      s"""case class ${name}(${fieldRendering.mkString(",")})"""
    }
  }
}


case class FunctionArg(name: String, tpe: ScalaType)

case class FunctionType(name: String, args: List[FunctionArg], returnType: ScalaType)

case class TableAPI(
                     caseClass: CaseClassType,
                     update: Option[FunctionType],
                     updateMany: Option[FunctionType],
                     insert: Option[FunctionType],
                     insertMany: Option[FunctionType],
                     delete: Option[FunctionType],
                     deleteMany: Option[FunctionType],
                     getById: Option[FunctionType],
                     getGeneric: List[FunctionType]
                   )

object Util {
  def typeTo__Type(tpe: Type): __Type = {
    tpe match {
      case Type.NamedType(name, nonNull) =>
        val inner = name match {
          case "String" => Types.string
          case "Int" => Types.int
          case "Boolean" => Types.boolean
          case "Long" => Types.long
          case "Float" => Types.float
          case "Double" => Types.double
        }
        if(nonNull) Types.makeNonNull(inner) else inner
      case Type.ListType(ofType, nonNull) => Types.makeList(typeTo__Type(ofType))
    }
  }

  def objectExtensionFieldToField(extension: ObjectExtensionField): __Field = {
    __Field(extension.fieldName, None, Nil, () => typeTo__Type(extension.fieldType), false, None, Option(List(Directive("customextension"))))
  }

  def checkExtensionApplies(tpe: __Type, extension :ObjectExtension): Boolean =  {

    val name = Util.extractTypeName(tpe)

    if(name.contentEquals(extension.objectName)) {
      //      println(s"checkExtensionApplies: ${name} -> ${extension.objectName} -> TRUE")
      true
    } else {
      //      println(s"checkExtensionApplies: ${name} -> ${extension.objectName} -> FALSE")
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

  def doesntReallyMatter(field: CField, extensions: Map[ObjectExtension, JsonObject => ZIO[Any,Nothing,JsonObject]]): List[Json => ZIO[Any, Nothing,Json]] = {
    extensions.flatMap { case (extension, transformer) =>
      checkIfExtensionUsed(field, extension, Nil).map(path => path -> transformer)
    }.map { case (path, transformer) =>
      println(s"PATH: $path has transformer")
      createModification(transformer, path)
    }.toList
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
}

object PostgresSniffer {
  def results[T](resultset: ResultSet)(f: ResultSet => T): List[T] = {
    new Iterator[T] {
      override def hasNext: Boolean = resultset.next()

      override def next(): T = f(resultset)
    }.toList
  }

  val typeMap = mutable.Map.empty[String, CaseClassType]

  def extractFunctionColumns(resultSet: ResultSet)(implicit c: Connection): FunctionColumnInfo = {
    FunctionColumnInfo(
      resultSet.getString("FUNCTION_NAME"),
      resultSet.getString("COLUMN_NAME"),
      resultSet.getShort("COLUMN_TYPE"),
      resultSet.getInt("DATA_TYPE"),
      resultSet.getString("TYPE_NAME"),
      resultSet.getShort("NULLABLE") match {
        case 0 => false
        case _ => true
      },
      resultSet.getString("REMARKS")
    )
  }

  def extractFunctions(resultSet: ResultSet)(implicit c: Connection): FunctionInfo = {
    FunctionInfo(
      resultSet.getString("FUNCTION_NAME"),
      resultSet.getString("REMARKS"),
      resultSet.getShort("FUNCTION_TYPE")
    )
  }

  def extractProcedureColumns(resultSet: ResultSet)(implicit c: Connection): ProcedureColumnInfo =  {
    ProcedureColumnInfo(
      resultSet.getString("PROCEDURE_NAME"),
      resultSet.getString("COLUMN_NAME"),
      resultSet.getShort("COLUMN_TYPE"),
      resultSet.getInt("DATA_TYPE"),
      resultSet.getShort("NULLABLE") match {
        case 0 => false
        case _ => true
      }
    )
  }

  def extractProcedures(resultSet: ResultSet)(implicit c: Connection): ProcedureInfo = {
    ProcedureInfo(
      resultSet.getString("PROCEDURE_NAME"),
      resultSet.getString("REMARKS")
    )
  }

  def extractPrimaryKey(resultSet: ResultSet)(implicit c: Connection): PrimaryKeyInfo = {
    val columnName = resultSet.getString("COLUMN_NAME")
    val keySeq = resultSet.getInt("KEY_SEQ")
    val name = resultSet.getString("PK_NAME")
    val tableName = resultSet.getString("TABLE_NAME")

    PrimaryKeyInfo(columnName, keySeq, tableName, name)
  }

  def extractTable(resultSet: ResultSet)(implicit c: Connection): TableInfo = {
    val catalog = Option(resultSet.getString("TABLE_CAT"))
    val schema = Option(resultSet.getString("TABLE_SCHEM"))
    val tableType = resultSet.getString("TABLE_TYPE")
    val comments = resultSet.getString("REMARKS")
    val name = resultSet.getString("TABLE_NAME")

    TableInfo(name, comments)
  }

  def extractColumn(resultSet: ResultSet): ColumnInfo = {
    val comments = resultSet.getString("REMARKS")
    val columnName = resultSet.getString("COLUMN_NAME")
    val tableName = resultSet.getString("TABLE_NAME")
    val dataTyp = resultSet.getInt("DATA_TYPE")
    val isNullable = resultSet.getString("IS_NULLABLE") match {
      case "YES" => true
      case "NO" => false
      case _ => true
    }
    val typeName = resultSet.getString("TYPE_NAME")
    ColumnInfo(columnName, isNullable, dataTyp, typeName, tableName)
  }

  def extractIndexInfo(resultSet: ResultSet): IndexInfo = {
    IndexInfo(
      resultSet.getString("TABLE_NAME"),
      resultSet.getString("INDEX_NAME"),
      resultSet.getInt("TYPE"),
      resultSet.getString("COLUMN_NAME"),
      resultSet.getBoolean("NON_UNIQUE")
    )
  }

  def extractExportedKeyinfo(resultSet: ResultSet): ExportedKeyInfo = {
    ExportedKeyInfo(
      resultSet.getString("PKTABLE_NAME"),
      resultSet.getString("PKCOLUMN_NAME"),
      resultSet.getString("FKTABLE_NAME"),
      resultSet.getString("FKCOLUMN_NAME"),
      resultSet.getInt("KEY_SEQ"),
      Option(resultSet.getString("PK_NAME")),
      Option(resultSet.getString("FK_NAME"))
    )
  }

  def extractImportedKeyinfo(resultSet: ResultSet): ImportedKeyInfo = {
    ImportedKeyInfo(
      resultSet.getString("PKTABLE_NAME"),
      resultSet.getString("PKCOLUMN_NAME"),
      resultSet.getString("FKTABLE_NAME"),
      resultSet.getString("FKCOLUMN_NAME"),
      resultSet.getInt("KEY_SEQ"),
      Option(resultSet.getString("PK_NAME")),
      Option(resultSet.getString("FK_NAME"))
    )
  }

  def toJVMType(dataType: Int): String = {
    dataType match {
      case 4 => "Int"
      case 12 => "String"
      case 1111 => "UUID"
      case 93 => "ZonedDateTime"
    }
  }

  def toScalaType(table: Table, columns: List[Column]): String = {
    val fields = columns.map { col =>
      val fieldType = if (col.nullable) s"Option[${toJVMType(col.sqltype)}]" else toJVMType(col.sqltype)
      s"${col.name}: ${fieldType}"
    }
    s"case class ${table.name}(${fields.mkString(",")})"
  }

  def toScalaScalar(sqlType: Int): BaseType = {
    sqlType match {
      case 4 => IntegerType
      case 12 => StringType
      case 1111 => UUIDType
      case 93 => ZonedDateTimeType
    }
  }

  def toScalaType(column: Column): ScalaType = {
    val innerType = column.sqltype match {
      case 4 => IntegerType
      case 12 => StringType
      case 1111 => UUIDType
      case 93 => ZonedDateTimeType
    }
    if (column.nullable) {
      OptionType(innerType)
    } else innerType
  }


  def toCaseClassType(table: Table)(tables: List[Table]): CaseClassType = {
    val primaryKeyFields = table.primaryKeys.map(col => Field(col.name, Concrete(toScalaType(col)), isPrimaryKey = true, isExtension = false, Nil))
    val otherColFields = table.otherColumns.map(col => Field(col.name, Concrete(toScalaType(col)), isPrimaryKey = false, isExtension = false, Nil))
    val relationshipFields = table.relationships.map { link =>

      if (link.pkTableName == table.name) {
        // Some other table references this table
        // Options are -> Option["other-type"], List["other-type"]

        // "other-type" cant occur. We are the primary
        // Option["other-type"] can occur. Our reference is unique and their reference is unique
        // List["other-type"] can occur. Our reference is non-unique or their reference is non-unique
        val otherTableName = if (link.pkTableName == table.name) link.fkTableName else link.pkTableName

        val thisTableReferenceName = if (link.pkTableName == table.name) link.pkColumnName else link.fkColumnName
        val thatTableReferenceName = if (link.pkTableName == table.name) link.fkColumnName else link.pkColumnName

        val thisTableReferenceUnique = (table.primaryKeys ++ table.otherColumns).find(_.name == thisTableReferenceName).map(_.unique).getOrElse(throw new RuntimeException(s"Unable to find column ${thisTableReferenceName} mentioned in $link"))
        val thatTable = tables.find(_.name == otherTableName).getOrElse(throw new RuntimeException(s"Unable to find table $otherTableName mentioned in ${link}"))
        val thatTableReferenceUnique = (thatTable.primaryKeys ++ thatTable.otherColumns).find(_.name == thatTableReferenceName).map(_.unique).getOrElse(throw new RuntimeException(s"Unable to find column ${thatTableReferenceName} mentioned in $link"))
        val linkType = if (thisTableReferenceUnique && thatTableReferenceUnique)
          () => OptionType(typeMap(otherTableName))
        else {
          () => ListType(typeMap(otherTableName))
        }
        val fieldName = if ((primaryKeyFields ++ otherColFields).exists(_.name == otherTableName)) s"${otherTableName}_ref" else otherTableName
        Field(fieldName, Reference(linkType), false, false, Nil)
      } else {
        // This table is foreign key, we are linking to some other table

        val otherTableName = if (link.pkTableName == table.name) link.fkTableName else link.pkTableName
        val thisTableReferenceName = if (link.pkTableName == table.name) link.pkColumnName else link.fkColumnName
        val thatTableReferenceName = if (link.pkTableName == table.name) link.fkColumnName else link.pkColumnName

        val thisTableReferenceNullable = (table.primaryKeys ++ table.otherColumns).find(_.name == thisTableReferenceName).map(_.nullable).getOrElse(throw new RuntimeException(s"Unable to find column ${thisTableReferenceName} mentioned in $link"))
        val thatTable = tables.find(_.name == otherTableName).getOrElse(throw new RuntimeException(s"Unable to find table $otherTableName mentioned in ${link}"))
        val thatTableReferenceUnique = (thatTable.primaryKeys ++ thatTable.otherColumns).find(_.name == thatTableReferenceName).map(_.unique).getOrElse(throw new RuntimeException(s"Unable to find column ${thatTableReferenceName} mentioned in $link"))

        // For "other-type" to occur our reference must be non-nullable and their reference must be unique
        // for Option["other-type"] to occur our reference must be nullable and their reference must be unique
        // for List["other-type"] to occur our reference must be nullable and their reference must not be unique
        // for NonEmptyList["other-type"] to occur our reference be non-nullable and their reference must not non unique
        // if this tables reference column is non-nullable AND that tables reference column is unique THEN "other-type"
        // if this tables reference column is nullable AND that tables reference column is unique THEN Option["other-type"]
        // if this tables reference column is nullable AND that tables reference column is not unique THEN List["other-type"]
        // if this tables reference column is non-nullable AND that tables reference column is not unique THEN NonEmptyList["other-type"]
        val linkType = (thisTableReferenceNullable, thatTableReferenceUnique) match {
          case (true, true) => () => OptionType(typeMap(otherTableName))
          case (true, false) => () => ListType(typeMap(otherTableName))
          case (false, false) => () => ListType(typeMap(otherTableName)) // Could refine to non-empty list
          case (false, true) => () => typeMap(otherTableName)
        }
        val fieldName = if ((primaryKeyFields ++ otherColFields).exists(_.name == otherTableName)) s"${otherTableName}_ref" else otherTableName
        Field(fieldName, Reference(linkType), false, false, Nil)
      }


    }
    val result = CaseClassType(table.name, primaryKeyFields ++ otherColFields ++ relationshipFields)
    typeMap.put(table.name, result)
    result
  }

  def caseClassToModel(caseClass: CaseClassType): String = caseClass.render

  def caseClassToInsertOp(caseClass: CaseClassType): FunctionType = {
    val input = CaseClassType(s"Create${caseClass.name}Input", caseClass.fields)
    FunctionType(s"insert_${caseClass.name}", List(FunctionArg("objectO", input)), caseClass) // TODO: Fix Object name
  }

  def caseClassToInsertManyOp(caseClass: CaseClassType): FunctionType = {
    val input = CaseClassType(s"Create${caseClass.name}Input", caseClass.fields)
    FunctionType(s"insert_many_${caseClass.name}", List(FunctionArg("object", ListType(input))), ListType(caseClass))
  }

  /** GraphQL does not support unions in inputs, so to update objects the normal way of doing it to do something like the following
   *
   * type User {
   * id: ID!
   * name: String
   * email: String!
   * }
   * input SetUserFields {
   * id: ID
   * name: String
   * email: String
   * }
   *
   * input UserPK {
   * id: ID!
   * }
   *
   * type UpdateUsersResponse {
   * affected_rows: Int!
   * returning: [User!]!
   * }
   *
   * type Mutation {
   * update_user(pk: UserPK!, updates: SetUserFields!): UpdateUsersResponse
   * }
   *
   * The server then interprets it such that all fields that are set will be updated. This means that
   *
   * {
   * name: null
   * }
   *
   * is interpreted differently than
   * {
   *
   * }
   *
   * as an update. The first object will explicitly delete the name, the send object will leave name unmodified.
   *
   * There is really no alternative to this except a method per possible combination of updates.
   * The reason for this is the lack of union support in input types. With union support in input types something like the following could be done.
   *
   * input UpdateUserName {
   * name: String
   * }
   *
   * input UpdateUserEmail {
   * email: String!
   * }
   *
   * input union UpdateUserField = UpdateUserName | UpdateUserEmail
   *
   * type Mutation {
   * update_user(pk: UserPK!, updates: [UpdateUserField!]!): UpdateUsersResponse
   * }
   */


  def caseClassToUpdateOp(caseClass: CaseClassType): FunctionType = {
    val pkSelectorCaseClass = CaseClassType(s"${caseClass.name}PK", caseClass.fields.filter(_.isPrimaryKey))
    val valueCaseClass = CaseClassType(s"Set${caseClass.name}Fields", caseClass.fields.filterNot(_.isPrimaryKey))
    val wrapperClassFields = List(
      Field("pk", Concrete(pkSelectorCaseClass), false, false, Nil),
      Field("set", Concrete(valueCaseClass), false, false, Nil)
    )
    val argWrapperClass = FunctionArg("arg", CaseClassType(s"Update${caseClass.name}Args", wrapperClassFields))

    // Caliban is unable to derive schemas when signatures are codegen.Field => (A,B) => Output
    // So we wrap (A,B) in C so the signature becomes codegen.Field => C => Output

    val responseFields = List(
      Field("affected_rows", Concrete(IntegerType), false, false, Nil),
      Field("rows", Concrete(caseClass), false, false, Nil)
    )

    val returnCaseClass = CaseClassType(s"Mutate${caseClass.name}Response", responseFields)

    FunctionType(s"update_${caseClass.name}", List(argWrapperClass), returnCaseClass)
  }

  // Requires the notion of filtering function
  def caseClassToUpdateManyOp(caseClass: CaseClassType): FunctionType = ???

  def caseClassToDeleteOp(caseClass: CaseClassType): FunctionType = {
    val pkSelectorCaseClass = CaseClassType(s"${caseClass.name}PK", caseClass.fields.filter(_.isPrimaryKey))

    FunctionType(s"delete_${caseClass.name}", List(FunctionArg("pk", pkSelectorCaseClass)), caseClass)
  }

  // Requires the notion of a filtering function
  def caseClassToDeleteManyOp(caseClass: CaseClassType): FunctionType = ???

  def caseClassToGetByIdOp(caseClass: CaseClassType): Option[FunctionType] = {
    caseClass.fields.filter(_.isPrimaryKey) match {
      case Nil => None
      case primaryKeys =>
        val pkSelectorCaseClass = CaseClassType(s"${caseClass.name}PK", primaryKeys)
        Option(FunctionType(s"get_${caseClass.name}_by_id", List(FunctionArg("pk", pkSelectorCaseClass)), OptionType(caseClass)))
    }

  }

  // Requires the notion of a filtering function
  def caseClassToSearchByValueOp(caseClass: CaseClassType, indexes: List[IndexInfo]): CaseClassType = ???


  def toTables(tableInfo: List[TableInfo], columns: List[ColumnInfo], primaryKeyInfo: List[PrimaryKeyInfo], exportedKeys: List[ExportedKeyInfo], importedKeys: List[ImportedKeyInfo], indexes: List[IndexInfo]): List[Table] = {
    tableInfo.map { table =>
      val tableIndexes = indexes.filter(_.tableName.contentEquals(table.name))
      val primaryKey = primaryKeyInfo.filter(v => v.tableName.contentEquals(table.name))
      val allColumns = columns.filter(_.tableName.contentEquals(table.name))
        .map(colInfo => Column(colInfo.name, colInfo.nullable, colInfo.sqltype, colInfo.typeName, tableIndexes.exists(index => index.columnName.contentEquals(colInfo.name) && !index.nonUnique)))
      val (primaryKeyColumn, otherColumns) = allColumns.partition(column => primaryKey.exists(_.columnName.contentEquals(column.name)))
      val links = exportedKeys.filter { key =>
        List(key.fkTableName, key.pkTableName).contains(table.name)
      }
      Table(table.name, primaryKeyColumn, otherColumns, table.comment, links)
    }
  }


  def extractComposite(caseClassType: CaseClassType): List[CaseClassType] = {
    caseClassType.fields.flatMap { field =>
      val fieldType = field.scalaType match {
        case Concrete(scalaType) => scalaType
        case Reference(scalaType) => scalaType()
      }

      fieldType match {
        case c: CaseClassType =>
          val recursive = c.fields
            .flatMap(v => extractAllCompositeTypes(v.scalaType match {
              case Concrete(scalaType) => scalaType
              case Reference(scalaType) => scalaType()
            }))
          List(c) ++ recursive
        case _ => Nil
      }
    }
  }

  def extractAllCompositeTypes(scalaType: ScalaType): List[CaseClassType] = {
    scalaType match {
      case c: CaseClassType =>
        extractComposite(c)
      case _ => Nil
    }
  }

  def toCalibanBoilerPlate(apis: List[TableAPI], extensions: List[ObjectExtension]) = {
    val allOps = apis.flatMap(api => api.delete.toList ++ api.insert.toList ++ api.update.toList ++ api.getById.toList)
    val readOps = apis.flatMap(api => api.getById.toList)
    val writeOps = apis.flatMap(api => api.delete.toList ++ api.insert.toList ++ api.update.toList)

    // Model

    val models = apis.map(api => api.caseClass.renderWithFederation()).mkString("\n")

    // Args
    val opArgs = apis.flatMap(api => (api.delete.toList.flatMap(_.args) ++ api.insert.toList.flatMap(_.args) ++ api.update.toList.flatMap(_.args) ++ api.getById.toList.flatMap(_.args)).map(_.tpe).distinct)
    val additional = opArgs.flatMap(v => extractAllCompositeTypes(v))
    println("ADDITIONAL")
    additional.foreach(println)
    println("ADDITIONAL")
    val args = (opArgs ++ additional).distinct.filterNot(arg => apis.map(_.caseClass).contains(arg))


    val argCode = args.map(_.render)

    // QueryRoot
    // TODO: Do something about tables with no
    val renderedQueryOps = readOps.map { functionType =>
      val renderedReturnType = functionType.returnType.name

      if (functionType.args.isEmpty) s"${functionType.name}: ${renderedReturnType}"
      else {
        val renderedArgs = functionType.args.map(_.tpe.name)
        s"${functionType.name}: (Field) =>  ${renderedArgs.mkString("(", ",", ")")} => ${renderedReturnType}"
      }
    }
    val queryRoot =
      s"""case class Queries (
         |${renderedQueryOps.map(str => s"\t$str").mkString(", \n")}
         |)""".stripMargin

    val renderedMutationOps = writeOps.map { functionType =>
      val renderedReturnType = functionType.returnType.name

      if (functionType.args.isEmpty) s"${functionType.name}: ${renderedReturnType}"
      else {
        val renderedArgs = functionType.args.map(_.tpe.name)
        s"${functionType.name}: (Field) =>  ${renderedArgs.mkString("(", ",", ")")} => ${renderedReturnType}"
      }
    }

    val mutationRoot =
      s"""case class Mutations(
         |${renderedMutationOps.map(str => s"\t${str}").mkString(", \n")}
         |)
         |""".stripMargin

    // Schema Implicits

    val outputTypes = apis.map(_.caseClass) ++ allOps.map(_.returnType)

    outputTypes.foreach(println)

    val schemaImplicits = outputTypes.flatMap { scalaType =>
      scalaType match {
        case CaseClassType(name, fields) => Some(s"implicit val ${name.toLowerCase()}Schema = gen[${name}]")
        case _ => None
      }
    }

    // Argbuilder Implicits

    val inputTypes = allOps.flatMap(_.args.map(_.tpe)).flatMap { scalaType =>
      scalaType match {
        case CaseClassType(name, fields) if fields.nonEmpty => Some(s"implicit val ${name.toLowerCase}Arg = ArgBuilder.gen[${name}]")
        case _ => None
      }
    }

    val renderedServiceFunctions = allOps.map { function =>
      val renderedFunctionArgs = List("field: codegen.Field") ++ function.args.map { arg =>
        s"${arg.name}: ${arg.tpe.name}"
      }
      s"def ${function.name}(${renderedFunctionArgs.mkString(",")}): ${function.returnType.name}"
    }


    //TODO: FIXservice name
    val serviceDefinition = {
      val serviceName = s"TestService"

      s"""
         |type ZIO${serviceName} = Has[$serviceName]
         |
         | trait ${serviceName} {
         |${renderedServiceFunctions.map(method => s"\t $method").mkString("\n")}
         | }
         |
         |val schema = new GenericSchema[ZIO${serviceName}]{}
         |import schema._
         |
         | def buildApi(query: Queries, mutation: Mutations) : GraphQL[Console with Clock with ZIO${serviceName}] =  {
         |   val api = graphQL(
         |     RootResolver(
         |       query,
         |       mutation
         |     )
         |   )
         |   api
         | }
         |
         |
         |""".stripMargin
    }

    val imports =
      """import caliban._
        |import caliban.GraphQL.graphQL
        |import caliban.execution.Field
        |import caliban.schema.{ArgBuilder, GenericSchema}
        |import caliban.schema.Schema.gen
        |import caliban.schema.Annotations.GQLDirective
        |import caliban.federation._
        |import zio.Has
        |import zio.clock.Clock
        |import zio.console.Console
        |
        |import java.time.ZonedDateTime
        |import java.util.UUID""".stripMargin

    val all = {
      List("// Models ") ++ List(models) ++
        List("// Output types ") ++ outputTypes.filterNot(output => apis.map(_.caseClass).contains(output)).filter {
        case CaseClassType(_, _) => true
        case _ => false
      }.map(_.render) ++
        List("// Arg code ") ++ argCode ++
        List("// QueryRoot ") ++ List(queryRoot) ++
        List("// MutationRoot ") ++ List(mutationRoot) ++
//        List("// Schema implicits ") ++ schemaImplicits ++
//        List("// Arg Builder implicits ") ++ inputTypes ++
        List("// Service definition ") ++ List(serviceDefinition)
    }

    (List(imports) ++ List("object Whatever {") ++ all.distinct ++ List("}")).mkString("\n")
  }

  def graphqlTypeToScalaType(tpe: Type): ScalaType = {
    val inner = tpe match {
      case Type.NamedType(name, nonNull) => name match {
        case "Int" => IntegerType
        case "String" => StringType
        case "ID" => UUIDType
      }
      case Type.ListType(ofType, nonNull) =>ListType(graphqlTypeToScalaType(ofType))
    }
    if(tpe.nullable) {
      OptionType(inner)
    } else {
      inner
    }

  }

  def toExtensionBoilerplate(tables: List[Table], tableAPIs: List[TableAPI], extensions: List[ObjectExtension]): String = {


    val extensionMethodSignatures = extensions.map{ extension =>
      val requiredFields =
        tableAPIs
          .find(_.caseClass.name.contentEquals(extension.objectName)).getOrElse(throw new RuntimeException(s"Unable to find tableAPI for extension ${extensions}"))
          .caseClass.fields.filter(tableField => extension.field.requiredFields.exists(_.contentEquals(tableField.name)))
      val inputTypes = requiredFields.map(_.scalaType match {
        case Concrete(scalaType) => scalaType.name
        case Reference(scalaType) => scalaType().name
      }).mkString("(", ",", ")")

      s"${extension.objectName}${extension.field.fieldName}Extension: $inputTypes => ZIO[Any,Nothing,${graphqlTypeToScalaType(extension.field.fieldType).name}]"
    }

    val extensionMethodNames = extensions.map(extension => s"${extension.objectName}${extension.field.fieldName}Extension")

    val extensionObjectHandlers = extensions.map { extension =>
      val requiredFields =
        tableAPIs
          .find(_.caseClass.name.contentEquals(extension.objectName)).getOrElse(throw new RuntimeException(s"Unable to find tableAPI for extension ${extensions}"))
          .caseClass.fields.filter(tableField => extension.field.requiredFields.exists(_.contentEquals(tableField.name)))

      val params = requiredFields.map { field =>
        s"""jsonObject.apply("${field.name}").get.as[${field.scalaType.toName()}].right.get"""
      }


      val jsonConverter = graphqlTypeToScalaType(extension.field.fieldType) match {
        case baseType: BaseType => baseType match {
          case UUIDType => "Json.fromString(output.toString())"
          case IntegerType => "Json.fromInt(output)"
          case StringType => "Json.fromString(output)"
          case ZonedDateTimeType => "Json.fromString(output.format(DateTimeFormatter.ISO_DATE_TIME))"
        }
        case containerType: ContainerType => ""
        case CaseClassType(name, fields) => ""
      }
      s"""def ${extension.objectName}${extension.field.fieldName}ExtensionConverter(jsonObject: JsonObject): ZIO[Any,Nothing,JsonObject] = {
         |  ${extension.objectName}${extension.field.fieldName}Extension(${params.mkString(",")}).map(output => jsonObject.add("${extension.field.fieldName}", $jsonConverter))
         |}""".stripMargin
    }

    //TODO: FIX Nil
    val extensionObjectDeclarations = extensions.map { extension =>
      s"""ObjectExtension("${extension.objectName}", ObjectExtensionField("${extension.field.fieldName}", Type.NamedType("${extension.field.fieldType.toString()}", nonNull = ${extension.field.fieldType.nonNull}), Nil)) -> ${extension.objectName}${extension.field.fieldName}ExtensionConverter"""
    }

    val imports =
      """
        |import Util.{argToWhereClause, doesntReallyMatter}
        |import caliban.ResponseValue.RawValue
        |import caliban.{GraphQL, InputValue, Value}
        |import caliban.introspection.adt.{__DeprecatedArgs, __Directive, __Type, __TypeKind}
        |import caliban.parsing.adt.Type
        |import caliban.schema.Step.{MetadataFunctionStep, ObjectStep}
        |import caliban.schema.{Operation, PureStep, RootSchemaBuilder, Schema, Step}
        |import generated.Whatever.Queries
        |import io.circe.{Json, JsonObject}
        |import zio.{UIO, ZIO}
        |import caliban.execution.{Field => CField}
        |import caliban.wrappers.Wrapper
        |
        |import io.circe.optics.JsonPath.root
        |import io.circe.optics.{JsonPath, JsonTraversalPath}
        |
        |import java.sql.{Connection, DriverManager}
        |import java.util.UUID
        |""".stripMargin

    val extensionLogicByTypeMap =
      s"""
         |    val extensionLogicByType: Map[ObjectExtension, JsonObject => ZIO[Any,Nothing,JsonObject]] = Map(
         |      ${extensionObjectDeclarations.mkString(",\n")}
         |    )
         |""".stripMargin

    val constrctOperationMethod = s"""
      |def constructOperation[R,Q](schema: Schema[R,Q])(implicit connection: Connection): Operation[R] = {
      |    ${extensionObjectHandlers.mkString("\n")}
      |
      |    $extensionLogicByTypeMap
      |
      |    val tpe = Util.extendType(schema.toType_(), extensionLogicByType.keys.toList)
      |
      |    def createFieldHandlers(a: __DeprecatedArgs): Map[String, MetadataFunctionStep[R]] = {
      |      val rootFields = tpe.fields(a).getOrElse(Nil)
      |
      |      rootFields.map { rootField =>
      |        def handler(field: CField): Step[R] = {
      |          // TODO: Possibly do some sort of requirement analysis
      |          // TODO: Create field mask
      |          // TODO: Add additional required fields
      |
      |          // We have created an intermediate that has filtered out all the fields that have @requires
      |          val intermediate = Util.toIntermediate(field, tables, "root", extensionLogicByType.keys.toList)
      |          val fieldMask: List[Json => Json] = Nil // Transformations for removing the fields that were added to satisfy extensions
      |
      |          val condition = field.arguments.map { case (k, v) => argToWhereClause(k, v) }.mkString(" AND ")
      |          val query = Util.toQueryWithJson(intermediate, "root", condition)
      |          val prepared = connection.prepareStatement(query)
      |
      |          println(query)
      |          val rs = prepared.executeQuery()
      |          rs.next()
      |          val resultJson = rs.getString("root")
      |          println(resultJson)
      |
      |          val json = io.circe.parser.parse(resultJson).getOrElse(Json.Null)
      |
      |          val modificaitons = doesntReallyMatter(field, extensionLogicByType)
      |
      |          val modifiedJson = modificaitons.foldLeft(ZIO.succeed(json)){ (acc, next) =>
      |            acc.flatMap(next(_))
      |          }
      |
      |          PureStep(RawValue(zio.Runtime.global.unsafeRun(modifiedJson).spaces4)) // TODO: FIx
      |        }
      |
      |        rootField.name -> MetadataFunctionStep(handler)
      |      }.toMap
      |    }
      |
      |    val queryFields = createFieldHandlers(__DeprecatedArgs(Option(true)))
      |    val resolver = ObjectStep(
      |      tpe.name.getOrElse("queries"),
      |      queryFields
      |    )
      |    Operation(tpe, resolver)
      |  }
      |""".stripMargin

    def tableToReprCondition(table: Table): String = {
      val primaryKeyConditions = table.primaryKeys.map { column =>
        val quoted = toScalaScalar(column.sqltype) match {
          case UUIDType => true
          case IntegerType => false
          case StringType => true
          case ZonedDateTimeType => true
        }

        if (quoted) s""""${column.name}" ++ " = '" ++ input.fields("${column.name}").toString.replaceAllLiterally(\"\\\"\", \"\") ++ "'" """
        else s""""${column.name}" ++ " = " ++ input.fields("${column.name}").toString.replaceAllLiterally(\"\\\"\", \"\") """

      }
      s"""def ${table.name}ReprToCondition(input: ObjectValue): String = {
         |  (${primaryKeyConditions.mkString(" AND ")})
         |}
         |""".stripMargin
    }

    val tpeToReprConditionMapEntries = tables.filter(_.primaryKeys.nonEmpty).map { table =>
      s""""${table.name}" -> ${table.name}ReprToCondition"""
    }.mkString(",\n")

    val tpeToReprConditionsMap =
      s"""
         |val tpeToConditionHandler: Map[String, ObjectValue => String] = Map(
         |  $tpeToReprConditionMapEntries
         |)
         |""".stripMargin

    def toFederatedBoilerplate(tables: List[Table]) = {


      /*
       val ordersEntityResolver = new EntityResolver[R] {
        override def resolve(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
          genericHandler(value)
        }

        override def toType: __Type = orderSchema.toType_()
      }
       */
      val entityResolvers = tables.filter(_.primaryKeys.nonEmpty).map { table =>
        s""" val ${table.name}EntityResolver = new EntityResolver[R] {
           |  override def resolve(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
           |        genericHandler(value)
           |      }
           |
           |  override def toType: __Type = Util.extendType(${table.name}Schema.toType_(), extensionLogicByType.keys.toList)
           |}
           |""".stripMargin
      }

      val entityResolverNames = tables.filter(_.primaryKeys.nonEmpty).map { table =>
        s"${table.name}EntityResolver"
      }

      val entityResolverList = entityResolverNames.tail.mkString(s"val entityResolverList = List(", ",", ")")

      val genericHandler = """
                             | def genericHandler(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
                             |      ZQuery.succeed(Step.MetadataFunctionStep(field => {
                             |
                             |        val intermediate = toIntermediateFromReprQuery(field, tables, "_entities", extensionLogicByType.keys.toList)
                             |
                             |        val otherArg = {
                             |          value match {
                             |            case InputValue.ListValue(values) => ""
                             |            case o @ ObjectValue(fields) =>
                             |              tpeToConditionHandler(fields("__typename").toInputString.replaceAllLiterally("\"", ""))(o)
                             |            case InputValue.VariableValue(name) => ""
                             |            case value: Value => ""
                             |          }
                             |        }
                             |        val condition = otherArg
                             |        val query = Util.toQueryWithJson(intermediate, "root", condition)
                             |        val prepared = connection.prepareStatement(query)
                             |
                             |        println(query)
                             |        val rs = prepared.executeQuery()
                             |        rs.next()
                             |        val resultJson = rs.getString("root")
                             |        println(resultJson)
                             |
                             |        val json = io.circe.parser.parse(resultJson).getOrElse(Json.Null)
                             |
                             |        val modificaitons = doesntReallyMatter(field, extensionLogicByType)
                             |
                             |        val modifiedJson = modificaitons.foldLeft(ZIO.succeed(json)){ (acc, next) =>
                             |          acc.flatMap(next(_))
                             |        }
                             |
                             |        val jsonRaw = zio.Runtime.global.unsafeRun(modifiedJson).noSpaces
                             |
                             |
                             |        PureStep(RawValue(jsonRaw)) // TODO: FIx
                             |
                             |      }))
                             |    }
                             |""".stripMargin

      val schemaImplicits = tables.filter(_.primaryKeys.nonEmpty).map { table =>
        s"${table.name}Schema: Schema[R, ${table.name}]"
      }

      s"""
         |def createFederatedApi[R](
         |    connection: Connection,
         |    ${extensionMethodSignatures.mkString(",")}
         |                           )(implicit querySchema: Schema[R, Queries], ${schemaImplicits.mkString(", ")}): GraphQL[R] = {
         |    val original = createApi(Nil, connection, ${extensionMethodNames.mkString(", ")})
         |
         |
         |   ${extensionObjectHandlers.mkString("\n")}
         |
         |   ${extensionLogicByTypeMap}
         |
         |   $genericHandler
         |
         |   ${entityResolvers.mkString("\n\n")}
         |
         |   $entityResolverList
         |
         |    federate(
         |      original,
         |      ${entityResolverNames.head},
         |      entityResolverList: _*
         |    )
         |  }""".stripMargin
    }

    s"""
      |$imports
      |
      |object ApiCreator {
      |
      |val tables = ${tables.map(_.renderSelf()).mkString("List(", ",", ")")}
      |
      |${tables.filter(_.primaryKeys.nonEmpty).map(tableToReprCondition).mkString("\n\n")}
      |
      |$tpeToReprConditionsMap
      |
      |${toFederatedBoilerplate(tables)}
      |
      |def createApi[R](
      |   directives: List[__Directive] = Nil,
      |   connection: Connection,
      |   ${extensionMethodSignatures.mkString(",")}
      |   )(
      |    implicit
      |    querySchema: Schema[R, Queries]
      |  ): GraphQL[R] = new GraphQL[R] {
      |
      |    $constrctOperationMethod
      |    override protected val schemaBuilder: RootSchemaBuilder[R] = RootSchemaBuilder(
      |      Option(
      |        constructOperation(querySchema)(connection)
      |      ),
      |      None,
      |      None
      |    )
      |    override protected val wrappers: List[Wrapper[R]] = Nil
      |    override protected val additionalDirectives: List[__Directive] = directives
      |  }
      |}
      |""".stripMargin
  }


  def connToTables(implicit conn: Connection): List[Table] = {
    val tablesRs = conn.getMetaData.getTables(null, null, null, Array("TABLE"))
    val tableInfo = results(tablesRs)(extractTable)

    val columnRs = conn.getMetaData.getColumns(null, null, null, null)
    val columnInfo = results(columnRs)(extractColumn)

    val primaryKeyInfo = tableInfo.flatMap { table =>
      val primaryKeyRs = conn.getMetaData.getPrimaryKeys(null, null, table.name)
      results(primaryKeyRs)(extractPrimaryKey)
    }

    val indexInfo = tableInfo.flatMap { table =>
      val indexRs = conn.getMetaData.getIndexInfo(null, null, table.name, false, true)
      val indexes = results(indexRs)(extractIndexInfo)
      indexes
    }

    val exportedKeyInfo = tableInfo.flatMap { table =>
      val exportRs = conn.getMetaData.getExportedKeys(null, null, table.name)
      results(exportRs)(extractExportedKeyinfo)
    }

    exportedKeyInfo.foreach(println)

    val importedKeyInfo = tableInfo.flatMap { table =>
      val importRs = conn.getMetaData.getImportedKeys(null, null, table.name)
      results(importRs)(extractImportedKeyinfo)
    }
    println()
    importedKeyInfo.foreach(println)
    println()

    toTables(tableInfo, columnInfo, primaryKeyInfo, exportedKeyInfo, importedKeyInfo, indexInfo)
  }

  def tablesToTableAPI(tables: List[Table]): List[TableAPI] = {
    tables.map(toCaseClassType(_)(tables)).map { tableCaseClass =>
      if(tableCaseClass.fields.exists(_.isPrimaryKey)) {
        TableAPI(
          tableCaseClass,
          Option(caseClassToUpdateOp(tableCaseClass)),
          null,
          Option(caseClassToInsertOp(tableCaseClass)),
          null,
          Option(caseClassToDeleteOp(tableCaseClass)),
          null,
          caseClassToGetByIdOp(tableCaseClass),
          null
        )
      } else {
        TableAPI(
          tableCaseClass,
          None,
          None,None, None,None, None, None, Nil
        )
      }
    }
  }

  def tablesToBoilerplate(tables: List[Table], extensions: List[ObjectExtension]) = {
    tables.map(toCaseClassType(_)(tables)).foreach(caseClass => println(caseClass.render))
    //TODO: Fix no primary key
    val apis = tablesToTableAPI(tables)

    toCalibanBoilerPlate(apis, extensions)
  }




  def fieldTypeToTspeName(fieldType: __Type, acc: String): String = {
    fieldType.ofType match {
      case Some(value) => fieldTypeToTspeName(value, s"$acc ${fieldType.kind.toString} of ")
      case None =>
        s"""$acc ${fieldType.kind.toString} named ${fieldType.name.getOrElse("WTF")}"""
    }
  }


  def checkListMatches(field: CField, extension: ObjectExtension): Boolean = {

    val typeName = Util.extractTypeName(field.fieldType)

    val res = typeName.contentEquals(extension.objectName) //&& field.fields.exists(_.name.contentEquals(extension.field.fieldName))
    println(s"CHECK MATCHES OBJECT $typeName = ${extension.objectName} -> $res")
    res
  }


  def constructOperation[R,Q](schema: Schema[R,Q], tables: List[Table])(implicit connection: Connection): Operation[R] = {

    def addressLogic(id: UUID): ZIO[Any, Nothing, String] = UIO(s"muhuhuha: ${id}")
    def addAddress(jsonObject: JsonObject): ZIO[Any, Nothing,JsonObject] = {
      addressLogic(jsonObject.apply("id").get.as[UUID].right.get).map(addrss => jsonObject.add("address", Json.fromString(addrss)))
    }

    val extensionLogicByType: Map[ObjectExtension, JsonObject => ZIO[Any, Nothing,JsonObject]] = Map(
      ObjectExtension("users", ObjectExtensionField("address", Type.NamedType("String", nonNull = true),Nil)) -> addAddress
    ) // Construct this in codegen

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

          val condition = field.arguments.map { case (k, v) => Util.argToWhereClause(k, v) }.mkString(" AND ")
          val query = Util.toQueryWithJson(intermediate, "root", condition)
          val prepared = connection.prepareStatement(query)

          println(query)
          val rs = prepared.executeQuery()
          rs.next()
          val resultJson = rs.getString("root")
          println(resultJson)

          // We have now received the result from the database
          // This result contains the queried for data - the fields from the extensions + the requirements from the extensions
          // Now we need to extract the arguments from the resultset
          val json = io.circe.parser.parse(resultJson).getOrElse(Json.Null)
          // We need to generate this code
          // So we have access some handlers here

          // So essentially we need a function with the following signare

          // That List[String] here is json paths
          // Should probably be some small sealed trait like 


          val modificaitons = Util.doesntReallyMatter(field, extensionLogicByType)

          val modifiedJson = modificaitons.foldLeft(ZIO.succeed(json)){ (acc, next) =>
            acc.flatMap(next(_))
          }

          // We can turn the field into a list of paths

//          json.hcursor.downField("users").downArray.values.get.map(_.hcursor.downField(""))


//          val modified = modifications.foldLeft(json)((acc, next) => next.apply(acc))

          // To make this work we need to generate the above modification list for the entire schema
          // then we can selectively apply the modification every time they are "enabled" by selecting the field



          // Essentially we need a way to select the places where the arguments are placed
          // Then for every instance of the argument we find, we need to compute extension value and splice it into the

          // We have got our result for all fields not marked with @requires
          // We need to resolve the rest
          // BUt how do we "get" this handler?
          // We are only able to extract the handler for the root query, but in this case we are
          // The real problem here is that the typing information is separate from the plan information.
          // If the plan information was present in the codegen.Field structure then it would be possible to
          // "zoom into a part of the query" and change/wrap execution of that part

          // Plan:
          // Add the transformations to the doStuff signature
          // I can then resolve the fields I know i can resolve in postgres
          // And the resolve the rest using some other method

//          println(modified.spaces4)
//          println("MODIFIED STUFFUFU")


          PureStep(RawValue(zio.Runtime.global.unsafeRun(modifiedJson).spaces4)) // TODO: FIx

          //Another possibilit is something like a SubSElect[A,B] typeclass made with magnolia

          // It seems that maybe the right solution is define this in codegen instead
          // So essentially you define your extension is regular graphql with a couple of directives that are quite close to federation
          // So an extends directive available on object
          // And an optional requires directive available on fields
          // Then you can create stub types and extend them with custom logic.
          // This should generate the types and an implementation for the base types and handlers for extending the base types with the custom logic

          // This makes sense because the base schema is generated at run time and not from some

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

  def withFederation[R, Q](tables: List[Table], connection: Connection)(
    implicit querySchema: Schema[R,Q],
    orerSchema: Schema[R, orders]
  ) = {
    val withoutFederation = doStuff2[R, Q](Nil, tables, connection)
    // This needs to be codegenned
    // The implementation looks like it can just reuse what we already have
    val ordersEntityResolver = new EntityResolver[R] {
      override def resolve(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
        ZQuery.succeed(Step.MetadataFunctionStep(field => PureStep(RawValue("{}"))))
      }

      override def toType: __Type = orerSchema.toType_()
    }

    val entityResolvers: List[EntityResolver[R]] = List(ordersEntityResolver)

    entityResolvers match {
      case ::(head, next) =>
        federate(withoutFederation, head, next:_*)
      case Nil =>
        federate(withoutFederation)
    }

  }

  def doStuff2[R, Q](directives: List[__Directive] = Nil, tables: List[Table], connection: Connection)(
    implicit
    querySchema: Schema[R, Q]
  ): GraphQL[R] = new GraphQL[R] {
    override protected val schemaBuilder: RootSchemaBuilder[R] = RootSchemaBuilder(
      Option(
        constructOperation(querySchema, tables)(connection)
      ),
      None,
      None
    )
    override protected val wrappers: List[Wrapper[R]] = Nil
    override protected val additionalDirectives: List[__Directive] = directives
  }

  def parseExtensions(): String =  {
    val extensionsFile = File("") / "src" / "main"/ "resources" / "extensions.graphql"

    val source = Source.fromFile(extensionsFile.toURI, "UTF-8")
    (for {
    line <- source.getLines()
    } yield line).mkString("")
  }

  def parseDocument(query: String) = {
    val sm = SourceMapper(query)
    val whatj = parse(query, document(_)).get.value
    whatj
  }

  def objectsInDocument(doc: ParsedDocument): List[ObjectTypeDefinition] = {
    doc.definitions.flatMap {
      case definition: Definition.ExecutableDefinition => Nil
      case definition: TypeSystemDefinition => definition match {
        case TypeSystemDefinition.SchemaDefinition(directives, query, mutation, subscription) =>  Nil
        case TypeSystemDefinition.DirectiveDefinition(description, name, args, locations) => Nil
        case definition: TypeDefinition => definition match {
          case t @ TypeDefinition.ObjectTypeDefinition(description, name, implements, directives, fields) =>
            Option(t)
          case TypeDefinition.InterfaceTypeDefinition(description, name, directives, fields) => Nil
          case TypeDefinition.InputObjectTypeDefinition(description, name, directives, fields) => Nil
          case TypeDefinition.EnumTypeDefinition(description, name, directives, enumValuesDefinition) => Nil
          case TypeDefinition.UnionTypeDefinition(description, name, directives, memberTypes) => Nil
          case TypeDefinition.ScalarTypeDefinition(description, name, directives) => Nil
        }
      }
      case extension: Definition.TypeSystemExtension => Nil
    }
  }

  def docToObjectExtension(doc: ParsedDocument): List[ObjectExtension]= {
    val objectDefinitions = doc.definitions.flatMap {
      case definition: Definition.ExecutableDefinition => Nil
      case definition: TypeSystemDefinition => definition match {
        case TypeSystemDefinition.SchemaDefinition(directives, query, mutation, subscription) =>  Nil
        case TypeSystemDefinition.DirectiveDefinition(description, name, args, locations) => Nil
        case definition: TypeDefinition => definition match {
          case TypeDefinition.ObjectTypeDefinition(description, name, implements, directives, fields) =>
            val objectExtensionsFields = fields.map { field =>
              ObjectExtensionField(fieldName = field.name, fieldType = field.ofType, requiredFields = field.directives.filter(_.name == "requires").map(_.arguments.values.head.toInputString.replaceAllLiterally("\"", "")))
            }

            objectExtensionsFields.map { objectExtensionsField =>
              ObjectExtension(name, objectExtensionsField)
            }
          case TypeDefinition.InterfaceTypeDefinition(description, name, directives, fields) => Nil
          case TypeDefinition.InputObjectTypeDefinition(description, name, directives, fields) => Nil
          case TypeDefinition.EnumTypeDefinition(description, name, directives, enumValuesDefinition) => Nil
          case TypeDefinition.UnionTypeDefinition(description, name, directives, memberTypes) => Nil
          case TypeDefinition.ScalarTypeDefinition(description, name, directives) => Nil
        }
      }
      case extension: Definition.TypeSystemExtension => Nil
    }
    objectDefinitions
  }


  def checkExtensionValid(extensions: List[ObjectExtension], baseApi: ParsedDocument): List[String] = {
    val objectsInBaseDoc = objectsInDocument(baseApi)
    extensions.flatMap { extension =>
      objectsInBaseDoc.find(_.name.contentEquals(extension.objectName)) match {
        case Some(baseObject) =>
          extension.field.requiredFields.flatMap { requiredField =>
            baseObject.fields.find(_.name.contentEquals(requiredField)) match {
              case Some(requiredFieldOnBaseObject) =>
                if(!extension.field.fieldType.nullable && requiredFieldOnBaseObject.ofType.nullable) List(s"Object ${baseObject.name} has been extended with field ${extension.field.fieldName} with a nonnullable type. This field requires ${requiredFieldOnBaseObject.name} which is nullable. This does not fly.")
                else Nil
              case None =>
                List(s"Obejct ${baseObject.name} does not have required field ${requiredField} needed for ${extension.field.fieldName}")
            }
          }
        case None => List(s"Object ${extension.objectName} does not exist")
      }
    }
  }
}

object Codegen extends App{
  import PostgresSniffer._
  val _ = classOf[org.postgresql.Driver]
  val con_str = "jdbc:postgresql://0.0.0.0:5438/product"
  implicit val conn = DriverManager.getConnection(con_str, "postgres", "postgres")

  val fileLocation = File("") / "src" / "main"/ "scala"
  val dir = File(fileLocation) / "generated"

  //dir.createDirectory()

  val outputFile = dir / "boilerplate.scala"

  val generatedBoilerPlate = tablesToBoilerplate(connToTables(conn), Nil)


  outputFile.toFile.writeAll("package generated \n",generatedBoilerPlate)

}

//object Extensions extends App {
//  import PostgresSniffer._
//  val _ = classOf[org.postgresql.Driver]
//  val con_str = "jdbc:postgresql://0.0.0.0:5438/product"
//  implicit val conn = DriverManager.getConnection(con_str, "postgres", "postgres")
//
//  val tables = connToTables(conn)
//
//  val extensionDoc = parseDocument(parseExtensions())
//  val extensions = docToObjectExtension(extensionDoc)
//
//  val baseApi = api.render
//  val baseDoc = parseDocument(baseApi)
//
//  println(baseApi)
//
//  val isExtensionValid = checkExtensionValid(extensions, baseDoc)
//  println(isExtensionValid)
//
//  println(toExtensionBoilerplate(tables, tablesToTableAPI(tables),extensions))
//}
//
//object Poker extends App {
//  import PostgresSniffer._
//  val _ = classOf[org.postgresql.Driver]
//  val con_str = "jdbc:postgresql://0.0.0.0:5438/product"
//  implicit val conn = DriverManager.getConnection(con_str, "postgres", "postgres")
//
//  val metadata = conn.getMetaData
//
//
//  val procedures = results(metadata.getProcedures(null, null, null))(extractProcedures)
//
//  println(s"PROCEDURES: ${procedures.size}")
//
//  val proceduresWithColumns = procedures.map { procedure =>
//    procedure -> results(metadata.getProcedureColumns(null, null, procedure.name, null))(extractProcedureColumns)
//  }
//
//  proceduresWithColumns.foreach { case (procedure, columns) =>
//    println(s"Procedure: ${procedure.name}")
//    columns.foreach { column =>
//      println(s"  ${column.columnName} -> ${column.columnType} -> ${column.dataType}")
//    }
//  }
//
//  val functions = results(metadata.getFunctions(null, null, null))(extractFunctions)
//
//  println(s"FUNCTIONS: ${functions.size}")
//
//  val functionsWithColumns = functions.filter(_.name.contentEquals("user_extended_info")).map { function =>
//    function -> results(metadata.getFunctionColumns(null, null, function.name, null))(extractFunctionColumns)
//  }
//
//  functionsWithColumns.foreach { case (procedure, columns) =>
//    println(s"Function: ${procedure.name}")
//    columns.foreach { column =>
//      println(s"  ${column.columnName} -> ${column.columnType} -> ${column.dataType} -> ${column.typeName}")
//    }
//  }
//
//}

//import zio._
//object Runner extends App {
//  import PostgresSniffer._
//  val _ = classOf[org.postgresql.Driver]
//  val con_str = "jdbc:postgresql://0.0.0.0:5438/product"
//  implicit val conn = DriverManager.getConnection(con_str, "postgres", "postgres")
//  import generated.Whatever._
//
//  val tables = connToTables(conn)
//
//  val api = doStuff2[Any, Queries](Nil,tables,conn)
//
//  val query =
//    """
//      |{
//      |  get_orders_by_id(id:"ebdc67f6-8925-4d98-948d-eb5042dc63fc") {
//      |    id
//      |    buyer
//      |    ordered_at
//      |    users {
//      |      id
//      |      name
//      |      age
//      |      email
//      |      address
//      |    }
//      |    lineitems {
//      |      product_id
//      |      amount
//      |      product {
//      |        upc
//      |        name
//      |        price
//      |        weight
//      |        review {
//      |          author
//      |          users {
//      |            id
//      |            name
//      |          }
//      |        }
//      |      }
//      |    }
//      |  }
//      |}
//      |""".stripMargin
//
//  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
//    (for {
//
//    interpreter <- api.interpreter
//    res <- interpreter.execute(query)
//    _ = println(io.circe.parser.parse(res.data.toString).right.get.spaces4)
////    _ = println(api.render)
//    } yield ()).exitCode
//  }
//}