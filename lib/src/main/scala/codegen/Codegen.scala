package codegen

import caliban.parsing.adt.Definition.{TypeSystemDefinition, TypeSystemExtension}
import caliban.parsing.adt.Definition.TypeSystemDefinition.TypeDefinition
import caliban.parsing.adt.Definition.TypeSystemDefinition.TypeDefinition.ObjectTypeDefinition
import caliban.parsing.adt.Definition.TypeSystemExtension.TypeExtension
import caliban.parsing.adt.{Definition, Directive, Document, Type}
import caliban._
import caliban.execution.{Field => CField}
import caliban.federation.{EntityResolver, federate}
import caliban.introspection.adt.{__DeprecatedArgs, __Directive, __Field, __Type, __TypeKind}
import caliban.parsing.{ParsedDocument, Parser, SourceMapper}
import caliban.schema.Step.{MetadataFunctionStep, ObjectStep, PureStep}
import caliban.schema.{Operation, RootSchemaBuilder, Schema, Step, Types}
import caliban.wrappers.Wrapper
import cats.Applicative
import codegen.PostgresSniffer.typeMap
import codegen.Util.{extractTypeName, typeToScalaType, typeTo__Type}
import generic.Util.{ExtensionPosition, genericQueryHandler}
//import codegen.Runner.api
import fastparse.parse
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
case class ObjectPath(fieldName: String, typeTest: Option[String]) extends JsonPathPart


case class ObjectExtensionField(fieldName: String, fieldType: ScalaType, requiredFields: List[String], isExternal: Boolean = false)
case class ObjectExtension(objectName: String, field: ObjectExtensionField)
case class ExternalField(name: String, tpe: Type, external: Boolean)
case class ExternalEntity(name: String, fields: List[ExternalField], keys: List[String])


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


// A table will at least be an object so no need to support scalars
sealed trait SQLResponseType {
  def wrapInErrorHandling(fieldNameOfObject: String, tableSql: String): String
}
case object ObjectResponse extends SQLResponseType {
  override def wrapInErrorHandling(fieldNameOfObject: String, tableSql: String): String =
    s"""
       |select coalesce((json_agg("$fieldNameOfObject") -> 0), 'null') as "$fieldNameOfObject"
       |from (
       |  $tableSql
       |) as "root"
       |""".stripMargin
}
case object ListResponse extends SQLResponseType {
  override def wrapInErrorHandling(fieldNameOfObject: String, tableSql: String): String =
    s"""
       |select coalesce(json_agg("$fieldNameOfObject"), '[]') as "$fieldNameOfObject"
       |from (
       |  $tableSql
       |) as "root"
       |""".stripMargin
}

case class IntermediateColumn(nameInTable: String, outputName: String) // What about function columns?
case class IntermediateJoinV2(leftColName: String, rightColName: String, right: IntermediateV2, name: String)

case class IntermediateFromV2(tableName: String, joins: List[IntermediateJoinV2]) // What about functions?
sealed trait IntermediateV2 {
  def from: IntermediateFromV2
  def cols: List[IntermediateColumn]
  def responseType: SQLResponseType
}
case class IntermediateV2Regular(
                                  cols: List[IntermediateColumn],
                                  from: IntermediateFromV2,
                                  responseType: SQLResponseType,
                                  condition: Option[String],
                                  ordering: Option[String]
                                ) extends IntermediateV2
case class IntermediateV2Union(
                                discriminatorFieldName: String,
                                discriminatorMapping: Map[String, List[IntermediateColumn]],
                                from: IntermediateFromV2,
                                responseType: SQLResponseType,
                                condition: Option[String],
                                ordering: Option[String]
                              ) extends IntermediateV2 {
  override def cols: List[IntermediateColumn] = Nil
}

case class IntermediateUnion(discriminatorColumn: String, columnsByDiscriminatorValue: Map[String, List[String]]) {
  def toSQL(field: CField) : String =  {
    val baseTableName = "whatever"
    val childrenTypesFetched = field.fields.flatMap(_.parentType).flatMap(_.name).distinct
    childrenTypesFetched match {
      case Nil => ""
      case ::(head, Nil) => ""
      case ::(head, tl) =>
        val childTypeSQL = columnsByDiscriminatorValue.map { case (tpe, columns) =>
          val queriedFieldsForThisType = field.fields.filter(_.parentType.flatMap(_.name).exists(_.contentEquals(tpe)))
          val columnEntries = columns
            .filter(column => queriedFieldsForThisType.exists(_.name.contentEquals(column)))
            .map { column =>
              s"""'$column', "${baseTableName}_data"."$column""""
            } ++ List(s"""'__typename', '${tpe}'""")

          s"""when "${baseTableName}_data"."$discriminatorColumn" = '${tpe}'
             |    then json_build_object(${columnEntries.mkString(",")})""".stripMargin
        }
        s"""select "json_spec"."object"
           |from (
           |  select case ${childTypeSQL.mkString("\n")}
           |              end as "object"
           |) as "json_spec"
           |
           |""".stripMargin
    }


  }
}

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

  def matchesName(name: String): Boolean = this.name.contentEquals(name)
}


case class Table(name: String, primaryKeys: List[Column], otherColumns: List[Column], comments: String, relationships: List[ExportedKeyInfo] = Nil) {
  def renderSelf(): String = {
    val renderedPrimaryKeys = primaryKeys.map(_.renderSelf()).mkString("List(", ",", ")")
    val renderedOtherColumns = otherColumns.map(_.renderSelf()).mkString("List(",",", ")")
    val renderedRelationships = relationships.map(_.renderSelf()).mkString("List(", ",", ")")
    s"""Table("${name}", $renderedPrimaryKeys, $renderedOtherColumns, "$comments", $renderedRelationships)"""
  }

  def matchesName(name: String): Boolean = this.name.contentEquals(name)
}


sealed trait SQLType

case object Integer extends SQLType

sealed trait ScalaType {
  def render: String

  def name: String

  def isQuoted: Boolean

  def to__Type: __Type
}

sealed trait BaseType extends ScalaType

case object UUIDType extends BaseType {
  override def render: String = "UUID"

  override def name: String = "UUID"

  override def isQuoted: Boolean = true

  override def to__Type: __Type = Types.makeScalar("ID",None)
}

case object IntegerType extends BaseType {
  override def render: String = "Int"

  override def name: String = "Int"

  override def isQuoted: Boolean = false

  override def to__Type: __Type = Types.int
}

case object StringType extends BaseType {
  override def render: String = "String"

  override def name: String = render

  override def isQuoted: Boolean = true

  override def to__Type: __Type = Types.string
}

case object ZonedDateTimeType extends BaseType {
  override def render: String = "ZonedDateTime"

  override def name: String = render

  override def isQuoted: Boolean = true

  override def to__Type: __Type = Types.makeScalar("ZonedDateTime")
}

sealed trait ContainerType extends ScalaType

case class ListType(elementType: ScalaType) extends ContainerType {
  override def render: String = s"List[${elementType.name}]"

  override def name: String = render

  override def isQuoted: Boolean = false

  override def to__Type: __Type = Types.makeList(elementType.to__Type)
}

case class OptionType(elementType: ScalaType) extends ContainerType {
  override def render: String = s"Option[${elementType.name}]"

  override def name: String = render

  override def isQuoted: Boolean = elementType.isQuoted

  override def to__Type: __Type = elementType.to__Type
}

sealed trait FieldType {
  def toName(): String
  def strict(): ScalaType
}

case class Concrete(scalaType: ScalaType) extends FieldType {
  override def toName(): String = scalaType.name

  override def strict(): ScalaType = scalaType

}

case class Reference(scalaType: () => ScalaType) extends FieldType {
  override def toName(): String = scalaType().name

  override def strict(): ScalaType = scalaType()
}

case class Field(name: String, scalaType: FieldType, isPrimaryKey: Boolean, isExtension: Boolean, requires: List[String], isRelationship: Boolean, isExternal: Boolean = false)

case class CaseClassType(name: String, fields: List[Field], isBaseTableClass: Boolean = false,  isInsertForTable: Option[String] = None, isUpdateForTable: Option[String] = None, isDeleteForTable: Option[String] = None, isFederationArg: Option[String] = None, externalKeys: Option[String] = None) extends ScalaType {
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

  def render2() = {
    val fieldRendering = fields.map { field =>
      val typeRendering = field.scalaType match {
        case Concrete(scalaType) => scalaType.name
        case Reference(scalaType) => scalaType().name
      }
      s"${field.name}: ${typeRendering}"
    }

    val caseClassDefinition = s"""case class ${name}(${fieldRendering.mkString(",")}) """

    val extendsClauses = List(
      if (isUpdateForTable.nonEmpty) Option("UpdateInput") else None,
      if (isInsertForTable.nonEmpty) Option("InsertInput") else None,
      if (isDeleteForTable.nonEmpty) Option("DeleteInput") else None,
      if(isFederationArg.nonEmpty) Option("FederationArg") else None
    ).flatten match {
      case Nil => ""
      case list => list.mkString("extends ", " with ", "")
    }

    val insertMethod = isInsertForTable match {
      case Some(value) => s"""def toInsert(): String = {
                             |  ${renderToInsert(value)}
                             | }""".stripMargin
      case None => ""
    }

    val updateMethod = isUpdateForTable match {
      case Some(value) =>
        s"""def toUpdate(): String = {
           |  ${renderToUpdate(value)}
           | }""".stripMargin
      case None => ""
    }

    val deleteMethod = isDeleteForTable match {
      case Some(value) =>
        s"""def toDelete(): String = {
           |  ${renderToDelete(value)}
           | }""".stripMargin
      case None => ""
    }

    val federationMethod = isFederationArg match {
      case Some(value) =>
        s"""def toSelect(): String =  {
           |  ${renderFederationArg()}
           | }""".stripMargin
      case None => ""
    }

    val convenienceMethods = List(insertMethod, updateMethod, deleteMethod, federationMethod).filter(_.nonEmpty)
    val implementation =
      if(convenienceMethods.isEmpty) ""
      else
        s"""{
           | ${convenienceMethods.mkString("\n")}
           |}
           |""".stripMargin

    val federationKeys = fields.filter(_.isPrimaryKey).map(_.name)

    val isInput = (isInsertForTable.toList ++ isUpdateForTable.toList ++ isDeleteForTable.toList).nonEmpty

    val federationKeyDirective = if(federationKeys.nonEmpty && isFederationArg.isEmpty && !isInput) s"""@GQLDirective(Key("${federationKeys.mkString(" ")}")) """ else ""

    s"""$federationKeyDirective$caseClassDefinition$extendsClauses$implementation""".stripMargin
  }

  def renderToInsert(tableName: String): String = {
    val columnList = fields.filterNot(_.isRelationship).map(_.name)
    val valueList = fields.filterNot(_.isRelationship).map { field =>
      val fieldName = "$"+field.name
      if(field.scalaType.strict().isQuoted) {
        s"'$fieldName'"
      } else {
        fieldName
      }
    }
    "s\""++s"insert into ${tableName}(${columnList.mkString(",")}) values (${valueList.mkString(",")})" ++ "\""
  }

  def renderToUpdate(tableName: String): String = "???"
  def renderToDelete(tableName: String): String = "???"

  def renderFederationArg(): String = {
    val predicates = fields.map {field =>
      val fieldName = "$"+field.name
        if(field.scalaType.strict().isQuoted) s"${field.name} = '$fieldName'"
        else s"${field.name} = $fieldName"
    }.mkString(" AND ")

    "s\"" ++ predicates ++ "\""
  }

  override def isQuoted: Boolean = false

  override def to__Type: __Type = {
    val f = fields.map { field =>
      val directives = if(field.isExternal) List(federation.External) else List()
      __Field(field.name, None, Nil, () => field.scalaType.strict().to__Type, false, None, Option(directives))
    }

    val directives = externalKeys.map(keys => List(federation.External, federation.Key(keys))).getOrElse(Nil)
    Types.makeObject(Option(name), None, f, directives,None)
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
          case str => typeMap(str).to__Type
        }
        if(nonNull) Types.makeNonNull(inner) else inner
      case Type.ListType(ofType, nonNull) => Types.makeList(typeTo__Type(ofType))
    }
  }

  def typeToScalaType(tpe: Type, alreadyWrapped: Boolean = false, external: Boolean): ScalaType = {
    tpe match {
      case Type.NamedType(name, nonNull) =>
        val base = name match {
          case "String" => StringType
          case "Int" => IntegerType
          case "ID" => if(external) StringType else UUIDType
          case other => typeMap(other)
        }
        if(!alreadyWrapped && !nonNull) {
          OptionType(base)
        } else {
          base
        }

      case Type.ListType(ofType, nonNull) => ListType(typeToScalaType(ofType, alreadyWrapped = true, external = external))
    }
  }

  def objectExtensionFieldToField(extension: ObjectExtensionField): __Field = {
    __Field(extension.fieldName, None, Nil, () => extension.fieldType.to__Type, false, None, None)
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


  def createModification(transformer: JsonObject => ZIO[Any, Nothing, JsonObject], path: List[JsonPathPart]): Json => ZIO[Any,Nothing, Json] = {
    import zio.interop.catz._
    import zio.interop.catz.implicits._

    path.foldLeft[PathWrapper](ObejctPathWrapper(root)) { (acc, next) =>
      next match {
        case ArrayPath => acc match {
          case ObejctPathWrapper(path) => ArrayPathWrapper(path.each)
          case ArrayPathWrapper(path) => ArrayPathWrapper(path.each)
        }
        case ObjectPath(fieldName, typeTest) => acc match {
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
      case InputValue.ListValue(values) => s"$key in [] " //TODO
      case InputValue.ObjectValue(fields) => "" //TODO
      case InputValue.VariableValue(name) => "" //TODO
      case value: Value => value match {
        case Value.NullValue => ""
        case value: Value.IntValue => s"$key = ${value}"
        case value: Value.FloatValue => s"$key = $value"
        case Value.StringValue(value) => s"$key = '${value}'"
        case Value.BooleanValue(value) => s"$key = $value"
        case Value.EnumValue(value) => s"$key = '$value'"
      }
    }
  }

  // Generic, straight copy
  def toIntermediateFromReprQuery(field: CField, tables: List[Table], layerName: String, extensions: List[ExtensionPosition]): Intermediate = {
    println(s"toIntermediateFromReprQuery: ${field.name}")
    // Top level entity query
    if(field.name.contentEquals("_entities")) {
      val tableName = field.fields.headOption.flatMap(_.parentType).flatMap(_.name).getOrElse(throw new RuntimeException("Unable to find table name for _entities request"))
      val table = tables.find(_.name.contentEquals(tableName.mkString(""))).getOrElse(throw new RuntimeException(s"Unable to find table for ${tableName.mkString("")}"))
      val extensionColumnsForThisTable = extensions.filter(_.objectName.contentEquals(table.name))
      val (queriedCols, recurseCases) = field.fields.partition(_.fields.isEmpty) // TODO: Do dependency analysis
      val extensionsForThisTable = extensions.filter(_.objectName.contentEquals(table.name))
      val requirementsFromExtensions = extensionsForThisTable.flatMap(_.requirements)
      // SELECT: id, name
      // FROM tablename
      val recursiveCases = recurseCases
        .filterNot(field => extensionColumnsForThisTable.map(_.fieldName).exists(_.contentEquals(field.name))) // Don't recurse on extension columns
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


      val cols = (queriedCols
        .filterNot(col => extensionColumnsForThisTable.map(_.fieldName).exists(_.contentEquals(col.name))).map(_.name)) ++ requirementsFromExtensions // Dont select extension columns, but do select the extensions requirements

      Intermediate(
        cols,
        IntermediateFrom(table.name, recursiveCases),
        field
          .copy(fieldType = field.fieldType.copy(kind = __TypeKind.OBJECT)) // TODO: Annoying hack needed to "cast" the output to an object due to calibans weird handling of entityresolvers
      )
    } else {
      toIntermediate(field, tables, layerName, extensions)
    }
  }

  def toIntermediate(field: CField, tables: List[Table], layerName: String, extensions: List[ExtensionPosition]): Intermediate = {
    println(s"toIntermediate: ${field.name}")
    val tableName = field.fieldType.ofType match {
      case Some(value) => compileOfType(value)
      case None => List(field.fieldType.name.getOrElse(throw new RuntimeException(s"No typename for field ${field}")))
    }

    val table = tables.find(_.name.contentEquals(tableName.mkString(""))).getOrElse(throw new RuntimeException(s"Unable to find table for ${tableName.mkString("")}"))
    val extensionColumnsForThisTable = extensions.filter(_.objectName.contentEquals(table.name))
    val (queriedCols, recurseCases) = field.fields.filterNot(_.name.contentEquals("__typename")).partition(_.fields.isEmpty) // TODO: Do dependency analysis
    val extensionsForThisTable = extensions.filter(_.objectName.contentEquals(table.name))
    val requirementsFromExtensions = extensionsForThisTable.flatMap(_.requirements)
    // SELECT: id, name
    // FROM tablename
    val recursiveCases = recurseCases.filterNot(field => extensionColumnsForThisTable.map(_.fieldName).exists(_.contentEquals(field.name)))
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



    Intermediate((queriedCols.filterNot(col => extensionColumnsForThisTable.map(_.fieldName).exists(_.contentEquals(col.name))).map(_.name) ++ requirementsFromExtensions).distinct, IntermediateFrom(table.name, recursiveCases), field)
  }

  def extractUnwrappedTypename(tpe: __Type): String = {
    tpe.kind match {
      case __TypeKind.SCALAR => tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.OBJECT => tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.INTERFACE => tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.UNION => tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.ENUM => tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.INPUT_OBJECT => tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.LIST => extractUnwrappedTypename(tpe.ofType.getOrElse(throw new RuntimeException("Should never happen")))
      case __TypeKind.NON_NULL => extractUnwrappedTypename(tpe.ofType.getOrElse(throw new RuntimeException("Should never happen")))
    }
  }

  def findTableFromFieldType(fieldType: __Type, tables: List[Table]): Table = {
    val unwrappedTypename = extractUnwrappedTypename(fieldType)
    tables.find(_.matchesName(unwrappedTypename)).getOrElse(throw new RuntimeException(s"Unable to find table $unwrappedTypename from type"))
  }

  def findTableFromFieldTypeReprQuery(field: CField, tables: List[Table]): Table = {
    val tableName = field.fields.headOption.flatMap(_.parentType).flatMap(_.name).getOrElse(throw new RuntimeException("Unable to find table name for _entities request"))
    tables.find(_.matchesName(tableName)).getOrElse(throw new RuntimeException(s"Unable to find table $tableName from type"))
  }

  def fieldToSQLResponseType(fieldType: __Type, nullable: Boolean): SQLResponseType = {
    fieldType.kind match {
      case __TypeKind.SCALAR => ???
      case __TypeKind.OBJECT  => ObjectResponse
      case __TypeKind.INTERFACE => ObjectResponse
      case __TypeKind.UNION => ObjectResponse
      case __TypeKind.ENUM => ???
      case __TypeKind.INPUT_OBJECT => ???
      case __TypeKind.LIST => ListResponse
      case __TypeKind.NON_NULL => fieldToSQLResponseType(fieldType.ofType.getOrElse(throw new RuntimeException("Should never happen")), false)
    }
  }

  def extractOrdering(field: CField): Option[String] = {
    //TODO
    None
  }

  def extractCondition(field: CField): Option[String] = {
    //TODO
    None
  }

  def matchesTypenameQuery(field: CField): Boolean = {
    field.name.contentEquals("__typename")
  }

  def toIntermediateV2(field: CField, tables: List[Table], layerName: String, extensions: List[ExtensionPosition], rootLevelOfEntityResolver: Boolean = false): IntermediateV2 = {
    // Find the table
    val table =
      if(rootLevelOfEntityResolver) findTableFromFieldTypeReprQuery(field, tables)
      else findTableFromFieldType(field.fieldType, tables)
    // Remove extension fields and __typename fields
    val sqlFields = field.fields
      .filterNot(matchesTypenameQuery)
      .filterNot(field => extensions.exists(_.matchesField(field, table.name)))
    // Find the columns we fetch from this table
    // Find the recursive cases
    val (tableFields, recursiveFields) = sqlFields
      .partition(field => (table.primaryKeys++table.otherColumns).exists(_.matchesName(field.name)))

    // Recurse on the recursive cases (joins)
    val joins = recursiveFields
      .map(field => (toIntermediateV2(field, tables, s"${layerName}.${field.name}", extensions), field))
      .map { case (otherTable, field) =>
        val otherTableName = otherTable.from.tableName

        // Find the link between the tables, it has to exist
        // First check if this table is the primary key table
        // Then check if this table is the foreign key table
        table.relationships
          .find { relationshipTable => relationshipTable.pkTableName == table.name && relationshipTable.fkTableName == otherTableName }
          .map(link => IntermediateJoinV2(link.pkColumnName, link.fkColumnName, otherTable, otherTableName))
          .orElse(table.relationships
            .find { relationshipTable => relationshipTable.fkTableName == table.name && relationshipTable.pkTableName == otherTableName}
            .map(link => IntermediateJoinV2(link.fkColumnName, link.pkColumnName, otherTable, otherTableName))
          )
          .getOrElse(throw new RuntimeException(s"Unable to find link between ${table.name} and ${otherTable} required by ${field}"))
      }

    // Fetch any columns the extensions depend on
    val usedExtensions = extensions.filter(extension => extension.objectName.contentEquals(table.name) && field.fields.exists(_.name.contentEquals(extension.fieldName)))
    val requirementsFromExtensions = usedExtensions.flatMap(_.requirements)
    // Caliban federation handling is weird
    val responseType = if(rootLevelOfEntityResolver) ObjectResponse else fieldToSQLResponseType(field.fieldType, nullable = true)
    val ordering = extractOrdering(field)
    val condition = extractCondition(field)

    val requirementsColumns = requirementsFromExtensions.map(requiredField => IntermediateColumn(requiredField, requiredField))
    val tableColumns = tableFields
      .map(field => field -> (table.primaryKeys ++ table.otherColumns).find(_.matchesName(field.name)))
      .map{case (field, column) => IntermediateColumn(column.getOrElse(throw new RuntimeException("Should never happen")).name, field.alias.getOrElse(field.name))}

    IntermediateV2Regular(requirementsColumns ++ tableColumns, IntermediateFromV2(table.name, joins),responseType, condition, ordering)

  }

  def intermdiateV2ToSQL(intermediate: IntermediateV2, fieldNameOfObject: String, additionalCondition: Option[String]): String = {
    val dataId = s"${fieldNameOfObject}_data"

    val thisTableSQL = intermediate match {
      case IntermediateV2Regular(cols, from, responseType, condition, ordering) =>
        /*
        select "json_spec"
        from (
          select "$dataId"."colName" as "outputName",
          select "$dataId"."colName" as "outputName",
          select "$dataId"."colName" as "outputName",
        )
       */
        val tableCols = cols.map { col =>
          s""""$dataId"."${col.nameInTable}" as "${col.outputName}""""
        }

        val joinCols = intermediate.from.joins.map { join =>
          val fieldName = join.name
          s""" "$fieldNameOfObject.$fieldName"."$fieldNameOfObject.$fieldName" as "$fieldName" """
        } ++ List(s"""'${from.tableName}' as "__typename"""")

        val effectiveCondition = (additionalCondition.toList ++ condition.toList) match {
          case Nil => ""
          case list => list.mkString(" where ", " AND " , "")
        }

          s"""
             |select row_to_json(
             |    (
             |      select "json_spec"
             |      from (
             |        select ${(tableCols ++ joinCols).mkString(",")}
             |      ) as "json_spec"
             |     )
             |) as "$fieldNameOfObject"
             |from (select * from ${from.tableName} $effectiveCondition) as "$dataId"
             |    """.stripMargin
      case IntermediateV2Union(discriminatorFieldName, discriminatorMapping, from, responseType, condition, ordering) =>
        /*
        select "json_spec"
        from (
          select case
                  when "$dataId"."$discriminatorFieldName" = '$discriminatorMapping[0]'
                    then json_build_object(
                      '$discriminatorMapping[0][0]${outputName}', "$dataId"."colName",
                      '$discriminatorMapping[0][0]${outputName}', "$dataId"."colName"
                    )
                  when "$dataId"."$discriminatorFieldName" = '$discriminatorMapping[1]'
                    then json_build_object(
                      '$discriminatorMapping[0][0]${outputName}', "$dataId"."colName",
                      '$discriminatorMapping[0][0]${outputName}', "$dataId"."colName"
                    )
                  else
                    json_build_object(
                      '$discriminatorMapping[0][0]${outputName}', "$dataId"."colName",
                      '$discriminatorMapping[0][0]${outputName}', "$dataId"."colName"
                    )
        )
       */

        discriminatorMapping.size match {
          case 0 => responseType match {
            case ObjectResponse => "select json_agg('null')"
            case ListResponse => "select json_agg('[]')"
          }
          case 1 =>
            //TODO:
            "regular table"
          case _ =>
            val last = discriminatorMapping.last
            val nMinusOne = discriminatorMapping.filterNot{case (k,_) => k.contentEquals(last._1)}

            val elseClause  = last._2.map{ col =>
              s"""'${col.outputName}', "$dataId"."${col.nameInTable}""""
            }++List(s"""'__typename', '${last._1}'""").mkString(
              s"""else
                 |  json_build_object(
                 |""".stripMargin,
              ",",
              ")"
            )

            val whenClauses = nMinusOne.map { case (discriminatorValue, columns)=>
              columns.map{ col =>
                s"""'${col.outputName}', "$dataId"."${col.nameInTable}""""
              }++List(s"""'__typename', '$discriminatorValue'""").mkString(
                s"""when "$dataId"."$discriminatorFieldName" = '$discriminatorValue'
                   |  then json_build_object(
                   |""".stripMargin,
                ",",
                ")"
              )
            }

            val subSelect = responseType match {
              case ObjectResponse => " -> 0"
              case ListResponse => ""
            }

            val effectiveCondition = (additionalCondition.toList ++ condition.toList) match {
              case Nil => ""
              case list => list.mkString("where ", " AND ", "")
            }

              s"""
                 |select json_agg(
                 |  select "json_spec"."object"
                 |  from (
                 |    select case
                 |      ${whenClauses.mkString("\n")}
                 |      $elseClause
                 |      end as "object"
                 |  ) as "json_spec"
                 |) $subSelect as $fieldNameOfObject
                 |from (select * from ${intermediate.from.tableName} $effectiveCondition) as "$dataId"
                 |""".stripMargin

        }
    }

    val joins = intermediate.from.joins.map { join =>
      (
        intermdiateV2ToSQL(join.right, s"""${fieldNameOfObject}.${join.name}""", Option(s""""$dataId"."${join.leftColName}" = "${join.rightColName}"""")),
        s"${fieldNameOfObject}.${join.right.from.tableName}"
      )
    }

    val thisTableWithJoins = joins
      .foldLeft(thisTableSQL)((acc, next) => {
        acc ++ s""" LEFT JOIN LATERAL (${next._1}) as "${next._2}" on true """
      })

    intermediate.responseType.wrapInErrorHandling(fieldNameOfObject, thisTableWithJoins)
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
  def toQueryWithJson(intermediate: Intermediate, fieldNameOfObject: String, condition: Option[String]): String = {
    val dataId = s"${fieldNameOfObject}_data"
    val typeName = List(s"'${intermediate.from.tableName}' as __typename")
    val cols = typeName ++ intermediate.cols.map { col =>
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
         |from (select * from ${intermediate.from.tableName} ${condition.map(cond => s"where $cond").getOrElse("")}) as "$dataId"
         |    """.stripMargin

    val joins = intermediate.from.joins.map { join =>
      (
        toQueryWithJson(join.right, s"""${fieldNameOfObject}.${join.right.field.name}""", Option(s""""$dataId"."${join.leftColName}" = "${join.rightColName}"""")),
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


  def toCaseClassType(table: Table, extensions: List[ObjectExtension])(tables: List[Table]): CaseClassType = {
    val primaryKeyFields = table.primaryKeys.map(col => Field(col.name, Concrete(toScalaType(col)), isPrimaryKey = true, isExtension = false, Nil, false))
    val otherColFields = table.otherColumns.map(col => Field(col.name, Concrete(toScalaType(col)), isPrimaryKey = false, isExtension = false, Nil, false))
    val applicableExtensions = extensions.filter(_.objectName.contentEquals(table.name)).map { objectExtension =>
      Field(objectExtension.field.fieldName, Concrete(objectExtension.field.fieldType), isPrimaryKey = false, isExtension = true, objectExtension.field.requiredFields, isRelationship = false, isExternal = objectExtension.field.isExternal)
    }
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
        Field(fieldName, Reference(linkType), isPrimaryKey = false, isExtension = false, Nil, isRelationship = true)
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
        Field(fieldName, Reference(linkType), isPrimaryKey = false, isExtension = false, Nil, isRelationship = true)
      }


    }
    val result = CaseClassType(table.name, primaryKeyFields ++ otherColFields ++ relationshipFields ++ applicableExtensions, isBaseTableClass = true)
    typeMap.put(table.name, result)
    result
  }

  def caseClassToModel(caseClass: CaseClassType): String = caseClass.render

  // TODO: Need to fix this excessive wrapping
  def caseClassToInsertOp(caseClass: CaseClassType): FunctionType = {
    val input = CaseClassType(s"Create${caseClass.name}Input", caseClass.fields.filterNot(_.isExtension).filterNot(_.isRelationship).filterNot(_.isExternal), isInsertForTable =Option(caseClass.name))
    FunctionType(s"insert_${caseClass.name}", List(FunctionArg("objectO", input)), caseClass) // TODO: Fix Object name
  }

  def caseClassToInsertManyOp(caseClass: CaseClassType): FunctionType = {
    val input = CaseClassType(s"Create${caseClass.name}Input", caseClass.fields.filterNot(_.isExtension).filterNot(_.isRelationship).filterNot(_.isExternal), isInsertForTable = Option(caseClass.name))
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
    val pkSelectorCaseClass = CaseClassType(s"${caseClass.name}PK", caseClass.fields.filter(_.isPrimaryKey), isUpdateForTable = Option(caseClass.name))
    val valueCaseClass = CaseClassType(s"Set${caseClass.name}Fields", caseClass.fields.filterNot(_.isPrimaryKey).filterNot(_.isExtension))
    val wrapperClassFields = List(
      Field("pk", Concrete(pkSelectorCaseClass), false, false, Nil, false),
      Field("set", Concrete(valueCaseClass), false, false, Nil, false)
    )
    val argWrapperClass = FunctionArg("arg", CaseClassType(s"Update${caseClass.name}Args", wrapperClassFields, isUpdateForTable = Option(caseClass.name)))

    // Caliban is unable to derive schemas when signatures are codegen.Field => (A,B) => Output
    // So we wrap (A,B) in C so the signature becomes codegen.Field => C => Output

    val responseFields = List(
      Field("affected_rows", Concrete(IntegerType), false, false, Nil, false),
      Field("rows", Concrete(caseClass), false, false, Nil, false)
    )

    val returnCaseClass = CaseClassType(s"Mutate${caseClass.name}Response", responseFields)

    FunctionType(s"update_${caseClass.name}", List(argWrapperClass), returnCaseClass)
  }

  // Requires the notion of filtering function
  def caseClassToUpdateManyOp(caseClass: CaseClassType): FunctionType = ???

  def caseClassToDeleteOp(caseClass: CaseClassType): FunctionType = {
    val pkSelectorCaseClass = CaseClassType(s"${caseClass.name}PK", caseClass.fields.filter(_.isPrimaryKey), isDeleteForTable = Option(caseClass.name))

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

  def usedTypes(tpe: ScalaType, acc: List[String]): List[String] = {
    tpe match {
      case baseType: BaseType => acc ++  List(baseType.name)
      case containerType: ContainerType => containerType match {
        case ListType(elementType) => usedTypes(elementType, acc)
        case OptionType(elementType) => usedTypes(elementType, acc)
      }
      case c: CaseClassType =>
        if(acc.contains(c.name)) {
          acc
        } else {
          c.fields.flatMap(field =>usedTypes(field.scalaType.strict(), acc ++ List(c.name)))
        }
    }
  }

  def to2CalibanBoilerPlate(apis: List[TableAPI], extensions: List[ObjectExtension], tables: List[Table], externalEntities: List[ExternalEntity]): String = {
    val imports =
      """
        |import caliban._
        |import caliban.GraphQL.graphQL
        |import caliban.execution.Field
        |import caliban.schema.{ArgBuilder, GenericSchema}
        |import caliban.schema.Schema.gen
        |import caliban.schema._
        |import caliban.schema.Annotations.GQLDirective
        |import caliban.InputValue.ObjectValue
        |import caliban.InputValue
        |import caliban.federation._
        |import caliban.introspection.adt.{__Directive, __Type}
        |import caliban.wrappers.Wrapper
        |import caliban.parsing.adt.Type
        |import generic._
        |import zio.ZIO
        |import zio.query.ZQuery
        |import generic.Util._
        |import io.circe.Json
        |import io.circe.generic.auto._, io.circe.syntax._
        |import codegen.{Table, Column, ExportedKeyInfo, ObjectExtension, ObjectExtensionField}
        |import java.sql.Connection
        |
        |import java.time.ZonedDateTime
        |import java.util.UUID
        |""".stripMargin

    val allOps = apis.flatMap(api => api.delete.toList ++ api.insert.toList ++ api.update.toList ++ api.getById.toList)
    val readOps = apis.flatMap(api => api.getById.toList)
    val writeOps = apis.flatMap(api => api.delete.toList ++ api.insert.toList ++ api.update.toList)

    // Model

    val models = apis.map(api => api.caseClass.render2()).mkString("\n")

    // External Entities

    val federatedEntities = externalEntities.map { externalEntity =>
      val fields = externalEntity.fields.map { field =>
        val annotations = if(field.external) s"@GQLDirective(External)" else ""
        s"$annotations ${field.name}: ${federatedGraphqlTypeToScalaType(field.tpe).name}"
      }
      s"""@GQLDirective(Extend) @GQLDirective(Key(${externalEntity.keys.mkString(" ")}))case class ${externalEntity.name}(${fields.mkString(",")})"""
    }

    val reachableTypenames = apis.map(api => api.caseClass).flatMap(usedTypes(_, Nil))
    val unreacheableTypes = externalEntities
      .filterNot { externalEntity => reachableTypenames.exists(_.contentEquals(externalEntity.name))}

    val unreachableTypeMethods = unreacheableTypes
      .map { externalEntity =>
        val typeName = externalEntity.name
        s"def create${typeName}Schema()(implicit schema: Schema[Any, ${typeName}]) : __Type = schema.toType_()"
      }

    val unreachableTypeInvocations = unreacheableTypes.map { unreachableType =>
      s"create${unreachableType.name}Schema()"
    }

    // Extension Args

    val extensionArgs = extensions.map { extension =>
      val caseClassName = s"${extension.objectName}${extension.field.fieldName}ExtensionArgs"
      val requiredFields =
        apis
          .find(_.caseClass.name.contentEquals(extension.objectName)).getOrElse(throw new RuntimeException(s"Unable to find tableAPI for extension ${extensions}"))
          .caseClass.fields.filter(tableField => extension.field.requiredFields.exists(_.contentEquals(tableField.name)))
      CaseClassType(caseClassName, requiredFields.map(_.copy(isPrimaryKey = false))).render2()
    }.mkString("\n")

    // Federation args

    val federationArgClasses = apis
      .filter(_.caseClass.fields.exists(_.isPrimaryKey))
      .map(api => api.caseClass.copy(name = s"${api.caseClass.name}FederationArg",isFederationArg = Option(api.caseClass.name),fields = api.caseClass.fields.filter(_.isPrimaryKey)))

    val federationArgs = federationArgClasses
      .map(_.render2()).mkString("\n")

    // Output types
    val outputTypes = (apis.map(_.caseClass) ++ allOps.map(_.returnType))
    .filterNot(output => apis.map(_.caseClass).contains(output)).filter {
      case c: CaseClassType => true
      case _ => false
    }.map(_.render)

    // Args
    val opArgs = apis
      .flatMap(api =>
        (api.delete.toList.flatMap(_.args) ++
          api.insert.toList.flatMap(_.args) ++
          api.update.toList.flatMap(_.args) ++
          api.getById.toList.flatMap(_.args)
          ).map(_.tpe).groupBy(_.name).flatMap(_._2.headOption))
    val additional = opArgs.flatMap(v => extractAllCompositeTypes(v))
    println("ADDITIONAL")
    additional.foreach(println)
    println("ADDITIONAL")
    val args = (opArgs ++ additional)
      .groupBy(_.name)
      .flatMap(_._2.headOption)
      .filterNot(arg => apis.map(_.caseClass).contains(arg))
      .filterNot(c => externalEntities.exists(_.name.contentEquals(c.name)))


    val argCode = args.map {
      case baseType: BaseType => baseType.render
      case containerType: ContainerType => containerType.render
      case c : CaseClassType => c.copy(fields = c.fields.filterNot(_.isRelationship)).render2
    }

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


    val extensionMethods = extensions.map { extension =>
      val methodName = s"${extension.objectName}${extension.field.fieldName}"
      val otherArgs = s"${extension.objectName}${extension.field.fieldName}ExtensionArgs"
      val returnType = extension.field.fieldType.name
      s"def $methodName(field: Field, args: $otherArgs): ZIO[Any,Throwable, $returnType]"
    }
    val extensionTrait =
      s"""trait Extensions {
        | ${extensionMethods.mkString("\n")}
        |}""".stripMargin

    val entityResolverMethods = apis.filter(_.caseClass.fields.exists(_.isPrimaryKey)).map{ api =>
      val params = "extensions: ExtensionLogicMap, tables: List[Table], conditionsHandlerMap: Map[String, ObjectValue => String], connection: Connection"
      val implicitParams = s"schema: Schema[Any,${api.caseClass.name}]"
      s"""def create${api.caseClass.name}EntityResolver($params)(implicit $implicitParams): EntityResolver[Any] = new EntityResolver[Any] {
         |    override def resolve(value: InputValue): ZQuery[Any, CalibanError, Step[Any]] = genericEntityHandler(extensions, tables, conditionsHandlerMap, connection)(value)
         |    override def toType: __Type = schema.toType_()
         |}""".stripMargin
    }

    val unreachableTypesObjet =
      s"""object UnreachableTypes {
         |   ${unreachableTypeMethods.mkString("\n")}
         |}""".stripMargin


    val entityResolvers =
      s"""object EntityResolvers {
        | ${entityResolverMethods.mkString("\n")}
        |}""".stripMargin

    val tableList = tables.map(_.renderSelf()).mkString("List(", ",", ")")

    val extensionLogicEntries = extensions.map { extension =>
      val extensionInputType = s"${extension.objectName}${extension.field.fieldName}ExtensionArgs"
      val extensionMethodName = s"${extension.objectName}${extension.field.fieldName}"
      val requiredFields = extension.field.requiredFields.map(reqField => s""""$reqField"""").mkString(",")
      s"""ExtensionPosition("${extension.objectName}","${extension.field.fieldName}", List($requiredFields)) -> middleGenericJsonObject[$extensionInputType,${extension.field.fieldType.name}](extensions.${extensionMethodName})("${extension.field.fieldName}")"""
    }

    val insertByFieldNameEntries = apis.flatMap(_.insert).map { insertOp =>
      s""""${insertOp.name}" -> convertInsertInput[${insertOp.args.head.tpe.name}]"""
    }
    val updateByFieldNameEntries = List("")
    val deleteByFieldNameEntries = List("")

    val entityResolverConditionsHandlerEntries = federationArgClasses.flatMap { federationArg =>
      federationArg.isFederationArg.map{ tableName =>
        s""""${tableName}" -> convertFederationArg[${federationArg.name}]"""
      }
    }

    val federationInvocationParams = apis.filter(_.caseClass.fields.exists(_.isPrimaryKey)).map { api =>
      s"""create${api.caseClass.name}EntityResolver(extensionLogicByType, tables, entityResolverConditionByTypeName, connection)"""
    } match {
      case ::(head, next) => s"baseApi,${head}, ${next.mkString(",")}"
      case Nil => "baseApi"
    }

    val apiClass =
      s"""class API(extensions: Extensions, connection: Connection)(implicit val querySchema: Schema[Any, Queries], mutationSchema: Schema[Any, Mutations]) {
         |   import EntityResolvers._
         |   import UnreachableTypes._
         |   val tables = $tableList
         |
         |   val extensionLogicByType: ExtensionLogicMap = List(
         |      ${extensionLogicEntries.mkString(",\n")}
         |   )
         |
         |   val insertByFieldName: Map[String, Json => String] = Map(
         |      ${insertByFieldNameEntries.mkString(",\n")}
         |   )
         |
         |   val updateByFieldName: Map[String, Json => String] = Map(
         |      ${updateByFieldNameEntries.mkString(",\n")}
         |   )
         |
         |   val deleteByFieldName: Map[String, Json => String] = Map(
         |      ${deleteByFieldNameEntries.mkString(",\n")}
         |   )
         |
         |   val entityResolverConditionByTypeName: Map[String, ObjectValue => String]= Map(
         |      ${entityResolverConditionsHandlerEntries.mkString(",\n")}
         |   )
         |
         |  val baseApi = new GraphQL[Any] {
         |    override protected val schemaBuilder: RootSchemaBuilder[Any] = RootSchemaBuilder(
         |      Option(
         |        constructQueryOperation[Any,Queries](
         |          connection,
         |          extensionLogicByType,
         |          tables
         |        )
         |      ),
         |      Option(
         |        constructMutationOperation[Any, Mutations](
         |          connection,
         |          extensionLogicByType,
         |          tables,
         |          insertByFieldName,
         |          updateByFieldName,
         |          deleteByFieldName
         |        )
         |      ),
         |      None
         |    )
         |    override protected val wrappers: List[Wrapper[Any]] = Nil
         |    override protected val additionalDirectives: List[__Directive] = Nil
         |  }
         |
         |    def createApi(
         |               ): GraphQL[Any] = {
         |    federate(
         |      $federationInvocationParams
         |    ).withAdditionalTypes(List(${unreachableTypeInvocations.mkString(", ")}))
         |  }
         |}
         |""".stripMargin

    s"""
       |$imports
       |
       |$models
       |
       |${federatedEntities.mkString("\n")}
       |
       |$extensionArgs
       |
       |$federationArgs
       |
       |${outputTypes.mkString("\n")}
       |
       |${argCode.mkString("\n")}
       |
       |$queryRoot
       |
       |$mutationRoot
       |
       |$extensionTrait
       |
       |$entityResolvers
       |
       |$unreachableTypesObjet
       |
       |$apiClass
       |""".stripMargin
  }

  def extractNameWithWrapper(tpe: __Type, wrapper: Option[String]): String = {
    tpe.kind match {
      case __TypeKind.SCALAR =>
        val base = tpe.name.get match {
          case "String" => "String"
          case "Integer" => "Int"
          case "ID" => "UUID"
        }
        wrapper.map(wrap => s"$wrap[$base]").getOrElse(base)
      case __TypeKind.OBJECT =>
        val base = extractTypeName(tpe)
        wrapper.map(wrap => s"$wrap[$base]").getOrElse(base)
      case __TypeKind.INTERFACE => ???
      case __TypeKind.UNION => ???
      case __TypeKind.ENUM => ???
      case __TypeKind.INPUT_OBJECT => ???
      case __TypeKind.LIST => extractNameWithWrapper(tpe.ofType.get, Option("List"))
      case __TypeKind.NON_NULL => extractNameWithWrapper(tpe.ofType.get, None)
    }
  }

  def graphqlTypeToScalaType(tpe: Type): ScalaType = {
    val inner = tpe match {
      case Type.NamedType(name, nonNull) => name match {
        case "Int" => IntegerType
        case "String" => StringType
        case "ID" => UUIDType
        case _ => typeMap(name)
      }
      case Type.ListType(ofType, nonNull) =>ListType(graphqlTypeToScalaType(ofType))
    }
    if(tpe.nullable) {
      OptionType(inner)
    } else {
      inner
    }
  }

  def federatedGraphqlTypeToScalaType(tpe: Type): ScalaType = {
    val inner = tpe match {
      case Type.NamedType(name, nonNull) => name match {
        case "Int" => IntegerType
        case "String" => StringType
        case "ID" => UUIDType
        case _ => typeMap(name)
      }
      case Type.ListType(ofType, nonNull) =>ListType(federatedGraphqlTypeToScalaType(ofType))
    }
    if(tpe.nullable) {
      OptionType(inner)
    } else {
      inner
    }
  }



  def connToTables(implicit conn: Connection): List[Table] = {
    val tablesRs = conn.getMetaData.getTables(null, null, null, Array("TABLE"))
    val tableInfo = results(tablesRs)(extractTable).filterNot(_.name.startsWith("hdb"))

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

  def tablesToTableAPI(tables: List[Table], extensions: List[ObjectExtension]): List[TableAPI] = {
    tables.map(toCaseClassType(_, extensions)(tables)).map { tableCaseClass =>
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



  def parseExtensions(extensionFileLocation: java.io.File): String =  {
    if(!extensionFileLocation.exists()) ""
    else {
      val source = Source.fromFile(extensionFileLocation.toURI, "UTF-8")
      (for {
        line <- source.getLines()
      } yield line).mkString("")
    }
  }

  def parseDocument(query: String): Document = {
    zio.Runtime.default.unsafeRun(Parser.parseQuery(query))
  }

  def objectsInDocument(doc: Document): List[ObjectTypeDefinition] = {
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

  def docToObjectExtension(doc: Document): List[ObjectExtension]= {
    val objectDefinitions = doc.definitions.flatMap {
      case definition: Definition.ExecutableDefinition => Nil
      case definition: TypeSystemDefinition => definition match {
        case TypeSystemDefinition.SchemaDefinition(directives, query, mutation, subscription) =>  Nil
        case TypeSystemDefinition.DirectiveDefinition(description, name, args, locations) => Nil
        case definition: TypeDefinition => definition match {
          case TypeDefinition.ObjectTypeDefinition(description, name, implements, directives, fields) =>
            val objectExtensionsFields = fields.map { field =>
              val requiredFields = field.directives.filter(_.name == "requires").map(_.arguments.values.head.toInputString.replaceAllLiterally("\"", ""))
              val isExternla = field.directives.exists(_.name.contentEquals("external"))
              val tpe = typeToScalaType(field.ofType, external = isExternla)
              ObjectExtensionField(fieldName = field.name, fieldType = tpe, requiredFields = requiredFields, isExternla)
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

  def docToFederatedInstances(doc: Document): List[ExternalEntity] = {
    doc.definitions.flatMap {
      case definition: Definition.ExecutableDefinition => Nil
      case definition: TypeSystemDefinition => Nil
      case extension: Definition.TypeSystemExtension => extension match {
        case TypeSystemExtension.SchemaExtension(directives, query, mutation, subscription) => Nil
        case extension: TypeSystemExtension.TypeExtension => extension match {
          case TypeExtension.ScalarTypeExtension(name, directives) => Nil
          case TypeExtension.ObjectTypeExtension(name, implements, directives, fields) =>
            val externalFields = fields.map { field =>
              ExternalField(field.name, field.ofType, field.directives.exists(_.name.contentEquals("external")))
            }
            val keys = directives
              .find(_.name.contentEquals("key"))
              .flatMap(_.arguments
                .get("fields"))
              .map(_.toInputString.replaceAllLiterally("\n", ""))
              .toList
              .flatMap(_.split(" "))

            val caseClassFields = fields.map{field =>
              codegen.Field(field.name, Reference(() => federatedGraphqlTypeToScalaType(field.ofType)), false, false, Nil, false)
            }

            typeMap.put(name,CaseClassType(name, caseClassFields, externalKeys = Option(keys.mkString(" "))))

            List(ExternalEntity(name, externalFields, keys))
          case TypeExtension.InterfaceTypeExtension(name, directives, fields) => Nil
          case TypeExtension.UnionTypeExtension(name, directives, memberTypes) => Nil
          case TypeExtension.EnumTypeExtension(name, directives, enumValuesDefinition) => Nil
          case TypeExtension.InputObjectTypeExtension(name, directives, fields) => Nil
        }
      }
    }
  }


  def checkExtensionValid(extensions: List[ObjectExtension], baseApi: Document): List[String] = {
    val objectsInBaseDoc = objectsInDocument(baseApi)
    extensions.flatMap { extension =>
      objectsInBaseDoc.find(_.name.contentEquals(extension.objectName)) match {
        case Some(baseObject) =>
          extension.field.requiredFields.flatMap { requiredField =>
            baseObject.fields.find(_.name.contentEquals(requiredField)) match {
              case Some(requiredFieldOnBaseObject) =>
                val extensionFieldNullable = extension.field.fieldType match {
                  case containerType: ContainerType => containerType match {
                    case ListType(elementType) => false
                    case OptionType(elementType) => true
                  }
                  case _ => false
                }
                if(!extensionFieldNullable && requiredFieldOnBaseObject.ofType.nullable) List(s"Object ${baseObject.name} has been extended with field ${extension.field.fieldName} with a nonnullable type. This field requires ${requiredFieldOnBaseObject.name} which is nullable. This does not fly.")
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


}

object Extensions extends App {
  import PostgresSniffer._
  val _ = classOf[org.postgresql.Driver]
  val con_str = "jdbc:postgresql://0.0.0.0:5438/product"
  implicit val conn = DriverManager.getConnection(con_str, "postgres", "postgres")

  val tables = connToTables(conn)

  val extensionsFile = File("") / "lib" / "src" / "main"/ "resources" / "extensions.graphql"

  val extensionDoc = parseDocument(parseExtensions(new java.io.File(extensionsFile.toURI)))
  val federatedStuff = docToFederatedInstances(extensionDoc)
  val extensions = docToObjectExtension(extensionDoc)

  extensions.foreach(println)



  federatedStuff.foreach(println)

  println(to2CalibanBoilerPlate(tablesToTableAPI(tables, extensions), extensions, tables, federatedStuff))

//  val baseApi = to2CalibanBoilerPlate(tablesToTableAPI(tables),extensions,tables)
//
//    println(baseApi)
//
//  val fileLocation = File("") / "lib" / "src" / "main"/ "scala"
//  val dir = File(fileLocation) / "codegen2"
//
//  dir.createDirectory()
//
//  val outputFile = dir / "boilerplate.scala"
//
//  val generatedBoilerPlate = to2CalibanBoilerPlate(tablesToTableAPI(tables), extensions, tables)
//
//  outputFile.toFile.writeAll("package codegen2 \n",generatedBoilerPlate)
}


object Render2 extends App {
  import PostgresSniffer._
  val _ = classOf[org.postgresql.Driver]
  val con_str = "jdbc:postgresql://0.0.0.0:5438/product"
  implicit val conn = DriverManager.getConnection(con_str, "postgres", "postgres")

  val tables = connToTables(conn)

  val extensionsFile = File("") / "lib" / "src" / "main"/ "resources" / "extensions.graphql"

  val extensionDoc = parseDocument(parseExtensions(new java.io.File(extensionsFile.toURI)))
  val extensions = docToObjectExtension(extensionDoc)
  val externalEntities = docToFederatedInstances(extensionDoc)

  val baseApi = to2CalibanBoilerPlate(tablesToTableAPI(tables, extensions),extensions,tables, externalEntities)

//  println(baseApi)

  val fileLocation = File("") / "lib" / "src" / "main"/ "scala"
  val dir = File(fileLocation) / "codegen2"

  //dir.createDirectory()

  val outputFile = dir / "boilerplate.scala"

  val generatedBoilerPlate = to2CalibanBoilerPlate(tablesToTableAPI(tables, extensions), extensions, tables, externalEntities)

  outputFile.toFile.writeAll("package codegen2 \n",generatedBoilerPlate)
}
//
object Poker extends App {
  import PostgresSniffer._
  val _ = classOf[org.postgresql.Driver]
  val con_str = "jdbc:postgresql://0.0.0.0:5438/product"
  implicit val conn = DriverManager.getConnection(con_str, "postgres", "postgres")

  val metadata = conn.getMetaData


  val procedures = results(metadata.getProcedures(null, null, null))(extractProcedures)

  println(s"PROCEDURES: ${procedures.size}")

  val proceduresWithColumns = procedures.map { procedure =>
    procedure -> results(metadata.getProcedureColumns(null, null, procedure.name, null))(extractProcedureColumns)
  }

  proceduresWithColumns.foreach { case (procedure, columns) =>
    println(s"Procedure: ${procedure.name}")
    columns.foreach { column =>
      println(s"  ${column.columnName} -> ${column.columnType} -> ${column.dataType}")
    }
  }

  val functions = results(metadata.getFunctions(null, null, null))(extractFunctions)

  println(s"FUNCTIONS: ${functions.size}")

  val functionsWithColumns = functions.filter(_.name.contentEquals("user_extended_info")).map { function =>
    function -> results(metadata.getFunctionColumns(null, null, function.name, null))(extractFunctionColumns)
  }

  functionsWithColumns.foreach { case (procedure, columns) =>
    println(s"Function: ${procedure.name}")
    columns.foreach { column =>
      println(s"  ${column.columnName} -> ${column.columnType} -> ${column.dataType} -> ${column.typeName}")
    }
  }

}

