package codegen

import caliban.parsing.adt.Definition.{TypeSystemDefinition, TypeSystemExtension}
import caliban.parsing.adt.Definition.TypeSystemDefinition.TypeDefinition
import caliban.parsing.adt.Definition.TypeSystemDefinition.TypeDefinition.ObjectTypeDefinition
import caliban.parsing.adt.Definition.TypeSystemExtension.TypeExtension
import caliban.parsing.adt.{Definition, Directive, Document, Type}
import caliban._
import caliban.execution.{Field => CField}
import caliban.federation.{EntityResolver, federate}
import caliban.introspection.adt.{__DeprecatedArgs, __Directive, __Field, __InputValue, __Type, __TypeKind}
import caliban.parsing.{ParsedDocument, Parser, SourceMapper}
import caliban.schema.Step.{MetadataFunctionStep, ObjectStep, PureStep}
import caliban.schema.{Operation, RootSchemaBuilder, Schema, Step, Types}
import caliban.wrappers.Wrapper
import cats.Applicative
import codegen.Util.{extractTypeName, typeToScalaTypeV2}
import codegen.Version2.{AbstractEffect, CaseClassType2, ContainerType, Field2, Method2, MethodParam, ObjectField, ScalaInt, ScalaList, ScalaOption, ScalaString, ScalaType, ScalaUUID, createRelationshipFields2, globalTypeMap}
import generic.Util.{ExtensionPosition, genericQueryHandler, printType}
import io.circe.Decoder
import zio.NonEmptyChunk

import java.{sql, util}
import java.sql.{Blob, CallableStatement, Clob, DatabaseMetaData, NClob, PreparedStatement, SQLWarning, SQLXML, Savepoint, Statement, Struct}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.Executor
import scala.collection.JavaConverters._
//import codegen.Runner.api
import fastparse.parse
import io.circe.optics.JsonPath.root
import io.circe.optics.{JsonPath, JsonTraversalPath}
import io.circe.{Json, JsonObject, parser}
import zio.query.ZQuery
import zio.{UIO, ZIO}
import io.circe.{Decoder, Encoder, Json, JsonObject}
import io.circe.generic.auto._
import io.circe.optics.JsonPath.root
import io.circe.syntax._

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.UUID
import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.io.File

sealed trait JsonPathPart
case object ArrayPath extends JsonPathPart
case class ObjectPath(fieldName: String, typeTest: Option[String])
    extends JsonPathPart

case class ExternalField(name: String, tpe: Type, external: Boolean)
case class ExternalEntity(
    name: String,
    fields: List[ExternalField],
    keys: List[String]
)

// Wrapper neccesary for fold the fold left to work
sealed trait PathWrapper
case class ObejctPathWrapper(path: JsonPath) extends PathWrapper
case class ArrayPathWrapper(path: JsonTraversalPath) extends PathWrapper

sealed trait JsonSelection
case class ObjectFieldSelection(fieldName: String) extends JsonSelection
case object ArraySelection extends JsonSelection

// A table will at least be an object so no need to support scalars
sealed trait SQLResponseType {
  def wrapInErrorHandling(fieldNameOfObject: String, tableSql: String): String
}
case object ObjectResponse extends SQLResponseType {
  override def wrapInErrorHandling(
      fieldNameOfObject: String,
      tableSql: String
  ): String =
    s"""
       |select coalesce((json_agg("$fieldNameOfObject") -> 0), 'null') as "$fieldNameOfObject"
       |from (
       |  $tableSql
       |) as "root"
       |""".stripMargin
}
case object ListResponse extends SQLResponseType {
  override def wrapInErrorHandling(
      fieldNameOfObject: String,
      tableSql: String
  ): String =
    s"""
       |select coalesce(json_agg("$fieldNameOfObject"), '[]') as "$fieldNameOfObject"
       |from (
       |  $tableSql
       |) as "root"
       |""".stripMargin
}

case class IntermediateColumn(
    nameInTable: String,
    outputName: String
) // What about function columns?
case class IntermediateJoinV2(
    leftColName: String,
    rightColName: String,
    right: IntermediateV2,
    name: String
)

case class IntermediateFromV2(
    tableName: String,
    joins: List[IntermediateJoinV2]
) // What about functions?
sealed trait IntermediateV2 {
  def from: IntermediateFromV2
  def cols: List[IntermediateColumn]
  def responseType: SQLResponseType
  def where: List[String]
  def pagination: Option[String]
  def fieldName: String
  def dataId: String = s"${fieldName}_data"
}
case class IntermediateV2Regular(
    cols: List[IntermediateColumn],
    from: IntermediateFromV2,
    responseType: SQLResponseType,
    ordering: Option[String],
    where: List[String],
    pagination: Option[String],
    fieldName: String
) extends IntermediateV2
case class IntermediateV2Union(
    discriminatorFieldName: String,
    discriminatorMapping: Map[String, List[IntermediateColumn]],
    from: IntermediateFromV2,
    responseType: SQLResponseType,
    ordering: Option[String],
    where: List[String],
    pagination: Option[String],
    fieldName: String
) extends IntermediateV2 {
  override def cols: List[IntermediateColumn] = Nil
}

case class FunctionInfo(name: String, comments: String, functionType: Short)

case class FunctionColumnInfo(
    functionName: String,
    columnName: String,
    columnType: Short,
    dataType: Int,
    typeName: String,
    nullable: Boolean,
    comments: String
)

case class ProcedureColumnInfo(
    procedureName: String,
    columnName: String,
    columnType: Short,
    dataType: Int,
    nullable: Boolean
)

case class ProcedureInfo(name: String, comments: String)

case class PrimaryKeyInfo(
    columnName: String,
    keySeq: Int,
    tableName: String,
    pkName: String
)

case class TableInfo(name: String, comment: String)

case class ColumnInfo(
    name: String,
    nullable: Boolean,
    sqltype: Int,
    typeName: String,
    tableName: String
) {
  import Version2._
  def toScalaType: Version2.ScalaType = {
    sqltype match {
      case 4    => ScalaInt
      case 12   => ScalaString
      case 1111 => ScalaUUID
      case 93   => ScalaZonedDataTime
    }
  }
}

case class IndexInfo(
    tableName: String,
    indexName: String,
    indexType: Int,
    columnName: String,
    nonUnique: Boolean
)

case class ExportedKeyInfo(
    pkTableName: String,
    pkColumnName: String,
    fkTableName: String,
    fkColumnName: String,
    keySeq: Int,
    pkName: Option[String],
    fkName: Option[String]
) {
  def renderSelf(): String = {
    val renderedPkName =
      pkName.map(name => s"""Some("${name}")""").getOrElse("None")
    val renderedFkName =
      fkName.map(name => s"""Some("$name")""").getOrElse("None")
    s"""ExportedKeyInfo("$pkTableName", "$pkColumnName", "$fkTableName", "$fkColumnName", $keySeq, $renderedPkName, $renderedFkName)"""
  }
}

case class ImportedKeyInfo(
    pkTableName: String,
    pkColumnName: String,
    fkTableName: String,
    fkColumnName: String,
    keySeq: Int,
    pkName: Option[String],
    fkName: Option[String]
)

case class Column(
    name: String,
    nullable: Boolean,
    sqltype: Int,
    typeName: String,
    unique: Boolean
) {
  import Version2._
  def renderSelf(): String = {
    s"""Column("$name", $nullable, $sqltype, "$typeName", $unique)"""
  }

  def matchesName(name: String): Boolean = this.name.contentEquals(name)

  def toScalaType(): Version2.ScalaType = {
    val baseType = sqltype match {
      case 4    => ScalaInt
      case 12   => ScalaString
      case 1111 => ScalaUUID
      case 93   => ScalaZonedDataTime
      case 2014 => ScalaInt
    }

    if (nullable) ScalaOption(baseType) else baseType
  }
}

sealed trait SQLType

case object Integer extends SQLType

object Whatever {
  sealed trait Table {
    def matchesName(name: String): Boolean
    def name: String
    def tableColumns: List[Column]
    def relationships: List[ExportedKeyInfo]
    def toPaginationParams: MethodParam
    def renderSelf: String
    def objectExtensions: List[Version2.ObjectExtension]

    def extractPaginationSizing(field: CField): String

    def extractExtensionArgTypes(): List[CaseClassType2] = {
      objectExtensions.flatMap { objectExtension =>
        extractExtensionArgType(objectExtension)
      }
    }

    def extractExtensionArgType(
        extension: Version2.ObjectExtension
    ): Option[CaseClassType2] = {
      extension.field.requiredFields match {
        case Nil => None
        case list =>
          val fields = list
            .flatMap(reqField =>
              this.tableColumns.find(_.matchesName(reqField))
            )
            .map(col => Field2(col.name, col.toScalaType(), Nil, Nil))
          Option(
            CaseClassType2(
              s"${extension.objectName}${extension.field.fieldName}ExtensionArg",
              fields,
              Nil,
              Nil
            )
          )
      }
    }

    def extractPkSelector(): Option[CaseClassType2]

    def extractArgTypes: List[CaseClassType2] = {
      extractPaginationArgTypes ++ extractPkSelector().toList ++ List(
        extractCreateInput
      )
    }

    def extractPaginationArgTypes: List[CaseClassType2]

    def toExtensionMethods(): List[Method2] = {
      this.objectExtensions.map { extension =>
        val params = extractExtensionArgType(extension).map { extensionArg =>
          MethodParam(extensionArg.name, extensionArg)
        }.toList
        Method2(
          s"${extension.objectName}${extension.field.fieldName}",
          List(params),
          Nil,
          extension.field.tpe
        )
      }
    }

    def extractPaginationCondition(field: CField): String

    def extractCreateInput: CaseClassType2 = {
      CaseClassType2(
        s"${name}CreateInput",
        this.tableColumns.map(col =>
          Field2(col.name, col.toScalaType(), Nil, Nil)
        ),
        Nil,
        Nil
      )
    }

    def toUnifiedModel(tables: List[Whatever.Table]): Version2.ScalaType

    def toCreateFieldName(): String = s"create_${name}"
    def toCreateField(tables: List[Whatever.Table]): Field2 = {
      val argClass = extractCreateInput
      Field2(
        toCreateFieldName(),
        ScalaOption(toUnifiedModel(tables)),
        Nil,
        List(MethodParam("args", argClass))
      )
    }

    def toInsert(field: CField): String = {
      val (cols, values) = tableColumns.flatMap { col =>
        CodegenUtils
          .extractColumnValueFromInputValue(col, field.arguments)
          .map(value => col.name -> value)
      }.unzip
      s"insert into $name(${cols.mkString(", ")}) values(${values.mkString(", ")})"
    }

    def toBulkCreateFieldName(): String = s"create_${name}_bulk"
    def toBulkCreateField(tables: List[Whatever.Table]): Field2 = {
      val argClass = extractCreateInput
      Field2(
        toBulkCreateFieldName(),
        ScalaList(toUnifiedModel(tables)),
        Nil,
        List(MethodParam("args", ScalaList(argClass)))
      )
    }

    def toBulkInsert(field: CField): String = {
      println(field.arguments)

      val (cols, values) = tableColumns.map { col =>
        col.name -> CodegenUtils.extractColumnValueFromInputValueWithList(
          col,
          field.arguments
        )
      }.unzip

      val valuesFormatted = values
        .transpose
        .map(list => list.mkString("(", ",", ")")).mkString("values", ",", "")

      s"insert into $name(${cols.mkString(",")}) $valuesFormatted"
    }
  }
  case class PrimaryKeyTable(
      name: String,
      primaryKeys: NonEmptyChunk[Column],
      otherColumns: List[Column],
      comments: String,
      relationships: List[ExportedKeyInfo],
      objectExtensions: List[Version2.ObjectExtension]
  ) extends Table {

    def extractFederatedCondition(input: InputValue): String = {
      primaryKeys
        .map { column =>
          val values = CodegenUtils.extractColumnsValueFromFederatedArguments(
            column,
            input
          )
          s"${column.name} in (${values.mkString(", ")})"
        }
        .mkString(" AND ")
    }

    def getByIdParams(): List[MethodParam] = {
      List(MethodParam("pk", toPk()))
    }

    def toModel(tables: List[Table]): CaseClassType2 = {
      val federationKey = primaryKeys.map(_.name).mkString("\"", " ", "\"")
      val federationAnnotation =
        s"""@GQLDirective(federation.Key($federationKey))"""
      val fields = this.tableColumns.map { column =>
        Field2(column.name, column.toScalaType(), Nil, Nil)
      }

      val extensionFields = objectExtensions.map { extension =>
        Field2(extension.field.fieldName, extension.field.tpe, Nil, Nil)
      }
      CaseClassType2(
        name,
        fields.toList ++ createRelationshipFields2(
          this,
          tables
        ) ++ extensionFields,
        List(federationAnnotation),
        Nil
      )
    }

    def toPk(): CaseClassType2 = {
      CaseClassType2(
        s"${name}_PK",
        primaryKeys
          .map(col => Field2(col.name, col.toScalaType(), Nil, Nil))
          .toList,
        Nil,
        Nil
      )
    }

    override def tableColumns: List[Column] =
      (primaryKeys.toList ++ otherColumns)

    def createPaginationParamType: CaseClassType2 = {
      CaseClassType2(
        s"${name}_PaginationParams",
        List(
          Field2("last", ScalaOption(toPk()), Nil, Nil),
          Field2("pageSize", ScalaInt, Nil, Nil)
        ),
        Nil,
        Nil
      )
    }

    def toPaginationParams(): MethodParam = {
      MethodParam("pagination", createPaginationParamType)
    }

    def extractPaginationCondition(field: CField): String = {
      println(
        s"extractPaginationCondition on ${field.arguments}. FOr table $name"
      )
      val last =
        field.arguments
          .get("last")
          .toList
          .flatMap { inputValue =>
            toPk().fields
              .flatMap { pk =>
                val res = CodegenUtils
                  .extactFieldValueFromObject(pk, inputValue)
                  .map(str => s"${pk.name} > ${str}")
                println(s"SYMBOLIC: ${pk.name} -> ${inputValue}. REAL $res")
                res
              }
          }

      println(s"LAST: $last")
      (last).mkString(" AND ")
    }
    def extractSingleItemCondition(field: CField): String = {
      primaryKeys.toList
        .flatMap { column =>
          CodegenUtils.extractColumnValueFromObject(
            column,
            field.arguments
          )
        }
        .mkString(" AND ")
    }

    override def renderSelf: String = {
      val renderedPrimaryKeys =
        primaryKeys.map(_.renderSelf()).mkString("NonEmptyChunk(", ", ", ")")
      val renderedOtherColumns =
        s"List(" ++ otherColumns.map(_.renderSelf()).mkString(",") ++ ")"
      val renderedRelationships = {
        s"List(" ++ relationships.map(_.renderSelf()).mkString(",") ++ ")"
      }
      val renderedExtensions =
        s"List(" ++ objectExtensions.map(_.renderSelf()).mkString(", ") ++ ")"
      s"""PrimaryKeyTable(
          "$name",
          $renderedPrimaryKeys,
          $renderedOtherColumns,
          "$comments",
          $renderedRelationships,
          $renderedExtensions
          )"""
    }

    override def extractPaginationArgTypes: List[CaseClassType2] = {
      List(createPaginationParamType, toPk())
    }

    override def extractPkSelector(): Option[CaseClassType2] = {
      Option(
        toPk()
      )
    }

    override def matchesName(name: String): Boolean =
      this.name.contentEquals(name)

    override def toUnifiedModel(tables: List[Table]): Version2.ScalaType =
      toModel(tables)

    override def extractPaginationSizing(field: CField): String = {
      val pageSize = field.arguments
        .get("pageSize")
        .flatMap(CodegenUtils.convertTo[Int](_))
        .getOrElse(25)

      s"limit $pageSize"
    }

    def toUpdateByIdFieldName(): String = s"update_${name}_by_pk"

    def toUpdateArg(): CaseClassType2 = {
      CaseClassType2(
        s"update_${name}_input",
        otherColumns.map { col =>
          val tpe = ScalaType.unwrap(col.toScalaType())
          Field2(col.name, ScalaOption(tpe), Nil, Nil)
        },
        Nil,
        Nil
      )
    }

    def toUpdateArgHolder(): CaseClassType2 =  {
      CaseClassType2(
        s"update_${name}_args",
        List(
          Field2("pk", toPk(), Nil, Nil),
          Field2("update", toUpdateArg(), Nil, Nil)
        ),
        Nil,
        Nil
      )
    }

    def toUpdateByIdField(tables: List[Whatever.Table]): Field2 = {
      val argClass = toUpdateArgHolder()
      Field2(
        toUpdateByIdFieldName(),
        toUnifiedModel(tables),
        Nil,
        List(MethodParam("args", argClass))
      )
    }

    def toUpdate(field: CField): String = {
      val where = primaryKeys.toList.flatMap { col =>
        val zomg = field.arguments.get("pk") match {
          case Some(value) => value match {
            case InputValue.ObjectValue(fields) => fields
            case _ => Map[String, InputValue]()
          }
          case None => Map[String, InputValue]()
        }
        CodegenUtils.extractColumnValueFromObject(col, zomg)
      } match {
        case Nil => ""
        case list => list.mkString(" AND ")
      }

      val updates = otherColumns.flatMap { col =>
        val moreZomg = field.arguments.get("update") match {
          case Some(value) => value match {
            case InputValue.ObjectValue(fields) => fields
            case _ => Map[String, InputValue]()
          }
          case None => Map[String, InputValue]()
        }
        CodegenUtils.extractColumnUpdateFromInputValue(col, moreZomg).map(value => s"${col.name} = $value")
      }.mkString(", ")

      s"update $name set $updates where $where"
    }
  }


  case class NonPrimaryKeyTable(
      name: String,
      columns: List[Column],
      comments: String,
      relationships: List[ExportedKeyInfo],
      objectExtensions: List[Version2.ObjectExtension]
  ) extends Table {
    override def tableColumns: List[Column] = columns

    override def renderSelf: String = {
      val renderedOtherColumns =
        s"List(" ++ columns.map(_.renderSelf()).mkString(",") ++ ")"
      val renderedRelationships =
        s"List(" ++ relationships.map(_.renderSelf()).mkString(",") ++ ")"
      val renderedExtensions =
        s"List(" ++ objectExtensions.map(_.renderSelf()).mkString(", ") ++ ")"

      s"""NonPrimaryKeyTable(
         |  "$name",
         |  $renderedOtherColumns,
         |  "$comments",
         |  $renderedRelationships,
         |  $renderedExtensions
         |)
         |""".stripMargin
    }

    def toModel(tables: List[Table]): CaseClassType2 = {
      val extensionFields = objectExtensions.map { extension =>
        Field2(extension.field.fieldName, extension.field.tpe, Nil, Nil)
      }

      CaseClassType2(
        name,
        columns.map(col =>
          Field2(col.name, col.toScalaType(), Nil, Nil)
        ) ++ createRelationshipFields2(this, tables) ++ extensionFields,
        Nil,
        Nil
      )
    }

    def createPaginationParamType: CaseClassType2 = {
      CaseClassType2(
        "NoPKPagination",
        List(
          Field2("offset", ScalaOption(ScalaInt), Nil, Nil),
          Field2("limit", ScalaInt, Nil, Nil)
        ),
        Nil,
        Nil
      )
    }

    def toPaginationParams(): MethodParam = {
      MethodParam("pagination", createPaginationParamType)
    }

    def extractPaginationCondition(field: CField): String = {
      val defaultLimit = 25
      val limit = field.arguments
        .get("limit")
        .flatMap(inputValue => CodegenUtils.convertTo[Int](inputValue))
        .getOrElse(defaultLimit)
      val offset = field.arguments
        .get("offset")
        .flatMap(inputValue => CodegenUtils.convertTo[Int](inputValue))

      (List(s"limit = $limit") ++ offset.map(offset => s"offset = $offset"))
        .mkString(" AND ")

      ""
    }

    override def extractPaginationArgTypes: List[CaseClassType2] = List(
      createPaginationParamType
    )

    override def extractPkSelector(): Option[CaseClassType2] = None

    override def matchesName(name: String): Boolean =
      this.name.contentEquals(name)

    override def toUnifiedModel(tables: List[Table]): Version2.ScalaType =
      toModel(tables)

    override def extractPaginationSizing(field: CField): String = {
      val defaultLimit = 25
      val offset = field.arguments
        .get("offset")
        .flatMap(inputValue => CodegenUtils.convertTo[Int](inputValue))

      val limit = field.arguments
        .get("limit")
        .flatMap(inputValue => CodegenUtils.convertTo[Int](inputValue))
        .getOrElse(defaultLimit)
      (offset.map(int => s"offset $int").toList ++ List(s"limit $limit"))
        .mkString(" ")
    }
  }
}

object CodegenUtils {
  def convertTo[A](
      inputValue: InputValue
  )(implicit decoder: Decoder[A]): Option[A] = {
    InputValue.circeEncoder.apply(inputValue).as[A].toOption
  }

  def extractColumnsValueFromFederatedArguments(
      column: Column,
      input: InputValue
  ): List[String] = {

    val columnValuesAsJson = input
      .asJson(InputValue.circeEncoder)
      .asObject
      .flatMap(_.apply(column.name))

    println(
      s"extractColumnsValueFromFederatedArguments: ${column.name}. $input $columnValuesAsJson"
    )

    ScalaType.unwrap(column.toScalaType()) match {
      case baseType: Version2.BaseType =>
        baseType match {
          case Version2.ScalaInt =>
            columnValuesAsJson
              .flatMap(_.as[Int].toOption)
              .map(int => s"$int")
              .toList
          case Version2.ScalaString =>
            columnValuesAsJson
              .flatMap(_.as[String].toOption)
              .map(str => s"'$str'")
              .toList
          case Version2.ScalaUUID =>
            columnValuesAsJson
              .flatMap(_.as[UUID].toOption)
              .map(uuid => s"'$uuid'")
              .toList
          case Version2.ScalaZonedDataTime =>
            columnValuesAsJson
              .flatMap(_.as[ZonedDateTime].toOption)
              .map(dt =>
                s"'${dt.format(DateTimeFormatter.ISO_ZONED_DATE_TIME)}'"
              )
              .toList
        }
      case _ => Nil
    }
  }

  def extractColumnValueFromInputValueWithList(
      column: Column,
      inputValue: Map[String, InputValue]
  ): List[String] = {

    val json =
      inputValue
        .get("value")
        .map(_.asJson(InputValue.circeEncoder))
        .flatMap(_.asArray)
        .getOrElse(Nil)
        .toList
        .flatMap(_.asObject)
        .flatMap(_.apply(column.name))

    println(
      s"extractColumnValueFromInputValue: ${column.name}. $inputValue $json"
    )
    ScalaType.unwrap(column.toScalaType()) match {
      case baseType: Version2.BaseType =>
        baseType match {
          case Version2.ScalaInt =>
            json
              .map(_.as[Int].toOption.map(int => s"$int").getOrElse("null"))
          case Version2.ScalaString =>
            json
              .map(
                _.as[String].toOption
                  .map(string => s"'$string'")
                  .getOrElse("null")
              )

          case Version2.ScalaUUID =>
            json.map(
              _.as[UUID].toOption
                .map(uuid => s"'${uuid.toString}'")
                .getOrElse("null")
            )

          case Version2.ScalaZonedDataTime =>
            json
              .map(
                _.as[ZonedDateTime].toOption
                  .map(dt =>
                    s"'${dt.format(DateTimeFormatter.ISO_ZONED_DATE_TIME)}'"
                  )
                  .getOrElse("null")
              )

        }
      case _ => Nil
    }
  }

  def extractColumnUpdateFromInputValue(
                                        column: Column,
                                        inputValue: Map[String, InputValue]
                                      ): Option[String] = {

    val json =
      inputValue.get(column.name).map(_.asJson(InputValue.circeEncoder))

    println(
      s"extractColumnUpdateFromInputValue: ${column.name}. $inputValue $json"
    )
    ScalaType.unwrap(column.toScalaType()) match {
      case baseType: Version2.BaseType =>
        baseType match {
          case Version2.ScalaInt =>
            json match {
              case Some(value) => if(value.isNull) Option("null") else value.as[Int].toOption.map(int => s"$int")
              case None => None
            }
          case Version2.ScalaString =>
            json match {
              case Some(value) => if(value.isNull) Option("null") else value.as[String].toOption.map(string => s"'$string'")
              case None => None
            }

          case Version2.ScalaUUID =>
            json match {
              case Some(value) => if(value.isNull) Option("null") else value.as[UUID].toOption.map(uuid => s"'${uuid.toString}'")
              case None => None
            }

          case Version2.ScalaZonedDataTime =>
            json match {
              case Some(value) => if(value.isNull) Option("null") else value.as[ZonedDateTime].toOption.map(dt =>
                s"'${dt.format(DateTimeFormatter.ISO_ZONED_DATE_TIME)}'"
              )
              case None => None
            }
        }
      case _ => None
    }
  }

  def extractColumnValueFromInputValue(
      column: Column,
      inputValue: Map[String, InputValue]
  ): Option[String] = {

    val json =
      inputValue.get(column.name).map(_.asJson(InputValue.circeEncoder))

    println(
      s"extractColumnValueFromInputValue: ${column.name}. $inputValue $json"
    )
    ScalaType.unwrap(column.toScalaType()) match {
      case baseType: Version2.BaseType =>
        baseType match {
          case Version2.ScalaInt =>
            json.flatMap(
              _.as[Int].toOption.map(int => s"$int")
            )
          case Version2.ScalaString =>
            json.flatMap(
              _.as[String].toOption
                .map(string => s"'$string'")
            )

          case Version2.ScalaUUID =>
            json.flatMap(
              _.as[UUID].toOption
                .map(uuid => s"'${uuid.toString}'")
            )

          case Version2.ScalaZonedDataTime =>
            json.flatMap(
              _.as[ZonedDateTime].toOption
                .map(dt =>
                  s"'${dt.format(DateTimeFormatter.ISO_ZONED_DATE_TIME)}'"
                )
            )

        }
      case _ => None
    }
  }

  def extractColumnValueFromObject(
      column: Column,
      inputValue: Map[String, InputValue]
  ): Option[String] = {

    val json =
      inputValue.get(column.name).map(_.asJson(InputValue.circeEncoder))

    println(s"extractColumnValueFromObject: ${column.name}. $inputValue $json")
    ScalaType.unwrap(column.toScalaType()) match {
      case baseType: Version2.BaseType =>
        baseType match {
          case Version2.ScalaInt =>
            json.flatMap(
              _.as[Int].toOption.map(int => s"${column.name} = $int")
            )
          case Version2.ScalaString =>
            json.flatMap(
              _.as[String].toOption
                .map(string => s"${column.name} = '$string'")
            )

          case Version2.ScalaUUID =>
            json.flatMap(
              _.as[UUID].toOption
                .map(uuid => s"${column.name} = '${uuid.toString}'")
            )

          case Version2.ScalaZonedDataTime =>
            json.flatMap(
              _.as[ZonedDateTime].toOption
                .map(dt =>
                  s"${column.name} = '${dt.format(DateTimeFormatter.ISO_ZONED_DATE_TIME)}'"
                )
            )

        }
      case _ => None
    }
  }

  def extactFieldValueFromObject(
      pk: Field2,
      inputValue: InputValue
  ): Option[String] = {
    val pkField = inputValue
      .asJson(InputValue.circeEncoder)
      .asObject
      .flatMap(_.apply(pk.name))
    println(s"PK: $pk INPUT: $inputValue PKFIELD: $pkField")
    ScalaType.unwrap(pk.scalaType) match {
      case baseType: Version2.BaseType =>
        baseType match {
          case Version2.ScalaInt =>
            pkField.flatMap(_.as[Int].toOption).map(int => s"$int")
          case Version2.ScalaString =>
            pkField
              .flatMap(_.as[String].toOption)
              .map(str => s"'$str'")
          case Version2.ScalaUUID =>
            pkField
              .flatMap(_.as[UUID].toOption)
              .map(uuid => s"'${uuid.toString}'")
          case Version2.ScalaZonedDataTime =>
            pkField
              .flatMap(_.as[ZonedDateTime].toOption)
              .map(dt =>
                s"'${dt.format(DateTimeFormatter.ISO_ZONED_DATE_TIME)}'"
              )
        }
      case _ => None
    }
  }
}

object Version2 {
  // Mutual referencing: Type A depends on type B who depends on type A
  // Mutual referencing is everywhere in graphql
  // This creates an initialization problem
  // If we want to create a type for A we need a type for B, but we haven't initialized B yet
  // If we then want to initialize B we need A, this presents a problem
  // Fortunately this is trivially fixed by introducing laziness
  // We just put all types in a map and instead of a field being a strict scala type its a lazy one
  // Once all types are initialized we can simply find the types in the typemap
  // This is fine or our usecase as long as we stick with the following rules
  // 1. during initialization we only put stuff into the map
  // 2. during rendering we fetch things out of the map
  // This approach is obviously not threadsafe, but concurrency is not really right now
  // The following alternatives were considered
  // - Statemonad or similar (invasive type signatures)
  // - Passing the map around (horribly error prone)
  type TableName = String
  val globalTypeMap = mutable.Map[TableName, ScalaType]()

  case class ObjectField(
      fieldName: String,
      tpe: ScalaType,
      requiredFields: List[String],
      isExternal: Boolean = false
  ) {
    def renderSelf(): String = {
      s"""ObjectField(
          "$fieldName", 
          ${tpe.renderSelf}, 
          ${requiredFields.map(str => s""""$str"""")},
          $isExternal)"""
    }
  }
  case class ObjectExtension(objectName: String, field: ObjectField) {
    def renderSelf(): String = {
      s"""ObjectExtension("$objectName", ${field.renderSelf()})"""
    }
  }

  sealed trait EffectWrapper {
    def wrap(name: String): String
  }
  case object ZQueryEffectWrapper extends EffectWrapper {
    override def wrap(name: String): String = s"ZQuery[Any, Throwable, $name]"
  }
  case object ZIOEffectWrapper extends EffectWrapper {
    override def wrap(name: String): String = s"ZIO[Any,Throwable, $name]"
  }
  case object NoEffectWrapper extends EffectWrapper {
    override def wrap(name: String): String = s"$name"
  }

  sealed trait ScalaType {
    def render(effect: EffectWrapper): String

    def nameWithWrapper(effect: EffectWrapper): String

    def name: String

    def isQuoted: Boolean

    def to__Type: __Type

    def to__InputValue: __InputValue

    def renderSelf: String

    def unwrapped(): ScalaType = {
      this match {
        case baseType: BaseType => baseType
        case containerType: ContainerType =>
          containerType match {
            case ScalaOption(inner)    => inner.unwrapped()
            case ScalaList(inner)      => inner.unwrapped()
            case AbstractEffect(inner) => inner.unwrapped()
          }
        case c: CaseClassType2  => c
        case TypeReference(tpe) => tpe().unwrapped()
      }
    }
  }

  object ScalaType {
    def unwrap(tpe: ScalaType): ScalaType = {
      tpe match {
        case baseType: BaseType => baseType
        case containerType: ContainerType =>
          containerType match {
            case ScalaOption(inner)    => unwrap(inner)
            case ScalaList(inner)      => unwrap(inner)
            case AbstractEffect(inner) => unwrap(inner)
          }
        case c: CaseClassType2  => c
        case TypeReference(tpe) => unwrap(tpe())
      }
    }
  }

  sealed trait BaseType extends ScalaType {
    override def render(effect: EffectWrapper): String = name

    override def nameWithWrapper(effect: EffectWrapper): String = name

    override def to__InputValue: __InputValue =
      __InputValue(name, None, () => to__Type, None, None)
  }

  case object ScalaInt extends BaseType {
    override def name: String = "Int"

    override def isQuoted: Boolean = false

    override def to__Type: __Type = Types.int

    override def renderSelf: String = "ScalaInt"
  }

  case object ScalaString extends BaseType {
    override def name: String = s"String"

    override def isQuoted: Boolean = true

    override def to__Type: __Type = Types.string

    override def renderSelf: String = "ScalaString"
  }

  case object ScalaUUID extends BaseType {
    override def name: String = "UUID"

    override def isQuoted: Boolean = true

    override def to__Type: __Type = Types.string

    override def renderSelf: String = "ScalaUUID"
  }

  case object ScalaZonedDataTime extends BaseType {
    override def name: String = "ZonedDateTime"

    override def isQuoted: Boolean = true

    override def to__Type: __Type = Types.makeScalar("ZonedDateTime")

    override def renderSelf: String = "ScalaZonedDateTime"
  }

  sealed trait ContainerType extends ScalaType
  case class ScalaOption(inner: ScalaType) extends ContainerType {
    override def name: String = s"Option[${inner.name}]"

    override def isQuoted: Boolean = inner.isQuoted

    override def to__Type: __Type = inner.to__Type

    override def render(effect: EffectWrapper): String = name

    override def nameWithWrapper(effect: EffectWrapper): String = name

    override def to__InputValue: __InputValue = inner.to__InputValue

    override def renderSelf: String = s"ScalaOption(${inner.renderSelf})"
  }

  case class ScalaList(inner: ScalaType) extends ContainerType {
    override def name: String = s"List[${inner.name}]"

    override def isQuoted: Boolean = true

    override def to__Type: __Type = Types.makeList(inner.to__Type)

    override def render(effect: EffectWrapper): String = name

    override def nameWithWrapper(effect: EffectWrapper): String = name

    override def to__InputValue: __InputValue = inner.to__InputValue

    override def renderSelf: String = s"ScalaList(${inner.renderSelf})"
  }

  case class AbstractEffect(inner: ScalaType) extends ContainerType {
    override def render(effect: EffectWrapper): String = inner.name

    override def nameWithWrapper(effect: EffectWrapper): String =
      effect.wrap(name)

    override def name: String = inner.name

    override def isQuoted: Boolean = false

    override def to__Type: __Type = inner.to__Type

    override def to__InputValue: __InputValue = inner.to__InputValue

    override def renderSelf: String = s"AbstractEffect(${inner.renderSelf})"
  }

  case class CaseClassType2(
      name: String,
      fields: List[Field2],
      annotations: List[String],
      extendsClauses: List[String]
  ) extends ScalaType {
    def render(effect: EffectWrapper): String = {
      val extensions = extendsClauses match {
        case Nil  => ""
        case list => s"extends ${list.mkString(" with ")}"
      }

      s"""${annotations.mkString("")} case class $name(${fields
        .map(_.render(effect))
        .mkString(",")}) $extensions"""
    }

    override def nameWithWrapper(effect: EffectWrapper): String =
      effect.wrap(name)

    override def isQuoted: Boolean = false

    override def to__Type: __Type = Types.makeObject(
      Option(name),
      None,
      fields.map(_.to__Field),
      Nil,
      None
    )

    override def to__InputValue: __InputValue = __InputValue(
      name,
      None,
      () => to__Type,
      None,
      None
    )

    override def renderSelf: String = {
      s"""CaseClassType2("$name", List(${fields
        .map(_.renderSelf())
        .mkString(", ")}), List(${annotations
        .map(str => s""""$str"""")
        .mkString(", ")}), List(${extendsClauses
        .map(str => s""""$str"""")
        .mkString(", ")}))"""
    }
  }

  case class TypeReference(tpe: () => ScalaType) extends ScalaType {
    override def render(effect: EffectWrapper): String = tpe().render(effect)

    override def nameWithWrapper(effect: EffectWrapper): String =
      tpe().nameWithWrapper(effect)

    override def name: String = tpe().name

    override def isQuoted: Boolean = tpe().isQuoted

    override def to__Type: __Type = tpe().to__Type

    override def to__InputValue: __InputValue = tpe().to__InputValue

    override def renderSelf: String = tpe().renderSelf
  }

  case class Field2(
      name: String,
      scalaType: ScalaType,
      annotations: List[String],
      arguments: List[MethodParam]
  ) {
    def render(effect: EffectWrapper): String = {
      val args = arguments match {
        case Nil           => ""
        case ::(head, Nil) => s"${head.tpe.name} =>"
        case list =>
          list.map(_.tpe.name).mkString("(", ", ", ") =>")
      }

      s"""${annotations.mkString("")} $name: $args ${scalaType.nameWithWrapper(
        effect
      )}"""
    }

    def to__Field: __Field = (
      __Field(name, None, Nil, () => scalaType.to__Type, false, None, None)
    )

    def renderSelf(): String = {
      s"""Field2("$name", ${scalaType.renderSelf}, List(${annotations
        .map(str => s""""$str"""")
        .mkString(", ")}), List(${arguments
        .map(_.renderSelf())
        .mkString(", ")}))"""
    }
  }

  case class MethodParam(name: String, tpe: ScalaType) {
    def render: String = {
      s"""$name: ${tpe.name}"""
    }

    def renderSelf(): String = {
      s"""MethodParam("$name", ${tpe.renderSelf})"""
    }
  }
  // List[List[MethodParam]] to support currying
  case class Method2(
      name: String,
      params: List[List[MethodParam]],
      implicits: List[MethodParam],
      returnType: ScalaType
  ) {
    def renderSignature(effectWrapper: EffectWrapper): String = {
      val renderedImplicits = implicits match {
        case Nil  => ""
        case list => list.mkString("(", ", ", ")")
      }
      s"def ${name}${params.map(_.map(_.render).mkString(",")).mkString("(", ")(", ")")}$renderedImplicits: ${effectWrapper
        .wrap(returnType.name)}"
    }
  }

  case class ScalaTrait(name: String, methods: List[Method2]) {
    def render(effectWrapper: EffectWrapper): String = {
      s"""trait $name {
         | ${methods.map(_.renderSignature(effectWrapper))}
         |}""".stripMargin
    }
  }

  case class GeneratedMethod(
      name: String,
      params: String,
      implicits: String,
      returnType: String,
      implementation: String
  ) {
    def render(effectWrapper: EffectWrapper): String = {
      val renderedImplicits = if (implicits.isEmpty) "" else s"($implicits)"
      s"""def $name${params
        .mkString("(", "", ")")}$renderedImplicits: $returnType =  {
         |  $implementation
         |}""".stripMargin
    }
  }

  case class CalibanAPI(
      models: List[CaseClassType2],
      inputOutputTypes: List[CaseClassType2],
      federationArgs: List[(Whatever.Table, CaseClassType2)],
      implicits: List[String], // <- given implicitly really, pun intended
      queryRoot: CaseClassType2,
      mutationRoot: CaseClassType2,
      unreachableTypes: List[GeneratedMethod], // Method
      entityResolvers: List[GeneratedMethod], // Method
      extensions: ScalaTrait,
      extensionArgTypes: List[CaseClassType2],
      tables: List[Whatever.Table],
      objectExtensions: List[ObjectExtension],
      mutationOps: List[
        (String, String, String)
      ] //Tablename, fieldName, methodName
  ) {
    def render(effectWrapper: EffectWrapper): String = {
      val renderedModels = models.map(_.render(effectWrapper)).mkString("\n")
      val renderedArgs =
        inputOutputTypes.distinct.map(_.render(NoEffectWrapper)).mkString("\n")

      val renderedExtensionArgTypes =
        extensionArgTypes.distinct.map(_.render(effectWrapper)).mkString("\n")
      val renderedImplicits = implicits.distinct.mkString("\n")
      val renderedQueryRoot = queryRoot.render(effectWrapper)
      val renderedQueryImplicit = ""
      val renderedMutationRoot = mutationRoot.render(effectWrapper)
      val renderedMutationImplicit = ""
      val renderedUnreachableTypes =
        s"""object UnreachableTypes {
           |  ${unreachableTypes.map(_.render(effectWrapper)).mkString("\n")}
           |}""".stripMargin
      val renderedUnreachablTypeInvocations = unreachableTypes
        .map { tpeCreator =>
          s"${tpeCreator.name}()"
        }
        .mkString("List(", ",", ")")

      val renderedEntityResolvers =
        s"""object EntityResolvers {
           |  ${entityResolvers.map(_.render(effectWrapper)).mkString("\n")}
           |}""".stripMargin

      val renderedExtensionTrait =
        s"""trait ${extensions.name} {
           | ${extensions.methods
          .map(_.renderSignature(effectWrapper))
          .mkString("\n")}
           |}""".stripMargin

      val renderedTableList =
        tables.map(_.renderSelf).mkString("List(", ",", ")")

      val renderedFederationInvocationParams = entityResolvers.map { method =>
        s"""${method.name}(extensionLogicByType, tables, entityResolverConditionByTypeName, connection)"""
      } match {
        case ::(head, next) => s"baseApi,${head}, ${next.mkString(",")}"
        case Nil            => "baseApi"
      }

      val renderedExtensionLogicMapEntries = objectExtensions.map { extension =>
        val extensionInputType =
          s"${extension.objectName}${extension.field.fieldName}ExtensionArg"
        val extensionMethodName =
          s"${extension.objectName}${extension.field.fieldName}"
        val requiredFields = extension.field.requiredFields
          .map(reqField => s""""$reqField"""")
          .mkString(",")
        s"""ExtensionPosition("${extension.objectName}","${extension.field.fieldName}", List($requiredFields)) -> middleGenericJsonObject[$extensionInputType,${extension.field.tpe.name}](extensions.${extensionMethodName})("${extension.field.fieldName}")"""
      }

      val renderedInsertByFieldNameEntries = mutationOps.map {
        case (table, opName, methodName) =>
          s""""$opName" -> $table.$methodName"""
      }
      val renderedUpdateByFieldNameEntries = List("")
      val renderedDeleteByFieldNameEntries = List("")

      val renderedTables = tables
        .map { table =>
          s"val ${table.name} = ${table.renderSelf}"
        }
        .mkString("\n")

      val renderedEntityResolverConditionsHandlerEntries = federationArgs
        .map { case (table, primaryKey) =>
          s""""${table.name}" -> ${table.name}.extractFederatedCondition"""
        }
        .mkString(",\n")

      val renderedApiClass =
        s"""class API(extensions: Extensions, connection: Connection) {
           |   import EntityResolvers._
           |   import UnreachableTypes._
           |   val tables = $renderedTableList
           |
           |   $renderedTables
           |
           |   val extensionLogicByType: ExtensionLogicMap = List(
           |      ${renderedExtensionLogicMapEntries.mkString(",\n")}
           |   )
           |
           |   val mutationOnByFieldName: Map[String, Field => String] = Map(
           |      ${renderedInsertByFieldNameEntries.mkString(",\n")}
           |   )
           |
           |   val entityResolverConditionByTypeName: Map[String, InputValue => String]= Map(
           |      $renderedEntityResolverConditionsHandlerEntries
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
           |          mutationOnByFieldName
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
           |      $renderedFederationInvocationParams
           |    ).withAdditionalTypes(${renderedUnreachablTypeInvocations})
           |  }
           |}
           |""".stripMargin

      s"""
         |${CalibanAPI.imports}
         |object Definitions {
         |
         |  $renderedModels
         |
         |  $renderedArgs
         |
         |  $renderedExtensionArgTypes
         |
         |$renderedImplicits
         |
         |$renderedQueryRoot
         |
         |$renderedMutationRoot
         |
         |$renderedQueryImplicit
         |
         |$renderedMutationImplicit
         |
         |$renderedUnreachableTypes
         |
         |$renderedEntityResolvers
         |
         |$renderedExtensionTrait
         |
         |$renderedApiClass
         |}
         |""".stripMargin
    }
  }

  object CalibanAPI {
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
        |import zio.NonEmptyChunk
        |import zio.query.ZQuery
        |import generic.Util._
        |import io.circe.Json
        |import io.circe.generic.auto._, io.circe.syntax._
        |import codegen.Whatever._
        |import codegen.{Column, ExportedKeyInfo}
        |import codegen.Version2._
        |import java.sql.Connection
        |
        |import java.time.ZonedDateTime
        |import java.util.UUID
        |""".stripMargin
  }

  case class GeneratedTypes(
      models: List[CaseClassType2],
      args: List[(String, CaseClassType2)],
      outputTypes: List[CaseClassType2],
      queryFields: List[Field2],
      mutationFields: List[(Field2, String, String, String)],
      entityResolvers: List[GeneratedMethod],
      extensionTraitMethods: List[Method2],
      entityResolverKeys: List[(Whatever.Table, CaseClassType2)]
  )

  // FIXME: Filter omitted tables before calling this method
  def toCalibanAPI(
      tables: List[Whatever.Table],
      externalExtensions: List[ObjectExtension]
  ): CalibanAPI = {

    val generatedTypes = tables
      .map { table =>
        // TODO: Name annotations

        val modelType = table match {
          case c: Whatever.PrimaryKeyTable     => c.toModel(tables)
          case c2: Whatever.NonPrimaryKeyTable => c2.toModel(tables)
        }

        val createFields = List(
          (
            table.toCreateField(tables),
            table.name,
            table.toCreateFieldName(),
            "toInsert"
          ),
          (
            table.toBulkCreateField(tables),
            table.name,
            table.toBulkCreateFieldName(),
            "toBulkInsert"
          )
        )
        val updateFields = List()
        val deleteFields = Nil
        val mutationFields = createFields ++ updateFields ++ deleteFields

        val extensionMethods = table.toExtensionMethods()

        table match {
          case prim: Whatever.PrimaryKeyTable =>
            val queryFields = List(
              Field2(
                s"get${modelType.name}ById",
                AbstractEffect(ScalaOption(modelType)),
                Nil,
                prim.getByIdParams()
              ),
              Field2(
                s"list${modelType.name}",
                AbstractEffect(ScalaList(modelType)),
                Nil,
                List(prim.toPaginationParams())
              )
              //TODO: Add more here, possibly based on annotations
            )

            val updateByIdField = List(
              (prim.toUpdateByIdField(tables), prim.name, prim.toUpdateByIdFieldName(), "toUpdate")
            )
            println(s"ZOMOZMOMZOGMZOMGOMOZMGZOMGZ ${updateByIdField.map(_._1.scalaType)}")


            GeneratedTypes(
              models = List(modelType),
              args = Nil,
              outputTypes = Nil,
              queryFields = queryFields,
              mutationFields = mutationFields ++ updateByIdField,
              entityResolvers =
                List(createEntityHandlerMethod(modelType)), // TODO: FIX
              extensionTraitMethods = extensionMethods,
              entityResolverKeys = List((table, prim.toPk()))
            )
          case noPrim: Whatever.NonPrimaryKeyTable =>
            GeneratedTypes(
              models = List(modelType),
              args = Nil,
              outputTypes = Nil,
              queryFields = Nil,
              mutationFields = mutationFields,
              entityResolvers = Nil,
              extensionTraitMethods = extensionMethods,
              entityResolverKeys = Nil
            )
        }
      }

    val queryRoot =
      CaseClassType2("Queries", generatedTypes.flatMap(_.queryFields), Nil, Nil)

    println("MUTATION FIELDS")
    generatedTypes.flatMap(_.mutationFields).foreach{case (a,b,c,d) => println(s"$c -> ${a.scalaType}")}

    val mutationRoot =
      CaseClassType2(
        "Mutations",
        generatedTypes.flatMap(_.mutationFields).map(v => v._1),
        Nil,
        Nil
      )

    // Fill up the global typemap
    tables.map {
      case table @ (prim: Whatever.PrimaryKeyTable) =>
        globalTypeMap.put(table.name, prim.toModel(tables))
      case table @ (noPrim: Whatever.NonPrimaryKeyTable) =>
        globalTypeMap.put(table.name, noPrim.toModel(tables))
    }

    val args = tables.flatMap { table =>
      table.extractArgTypes
    }

    val outputTypes =
      Nil //TODO: Add these later when we implement more complicated ops

    val extensionArgTypes = tables.flatMap { table =>
      table.extractExtensionArgTypes()
    }

    val unreachableTypeMethods = externalExtensions
      .map(_.field.tpe)
      .filter(needsSchema)
      .filterNot(tpe => globalTypeMap.values.exists(v => v == tpe))
      .map(tpe =>
        //def create${typeName}Schema()(implicit schema: Schema[Any, ${typeName}]) : __Type = schema.toType_()"
        Version2.GeneratedMethod(
          s"create${tpe.name}Schema",
          "",
          s"schema: Schema[Any, ${tpe.name}]",
          "__Type",
          "schema.toType_()"
        )
      )

    val extensionTrait = ScalaTrait(
      "Extensions",
      generatedTypes.flatMap(_.extensionTraitMethods)
    )
    val schemaImplicits =
      (generatedTypes.flatMap(_.models) ++ args ++ outputTypes).map { tpe =>
        s"implicit val ${tpe.name}Schema = Schema.gen[${tpe.name}]"
      }

    val argbuilderImplicits = args.map { argTpe =>
      s"implicit val ${argTpe.name}ArgBuilder = ArgBuilder.gen[${argTpe.name}]"
    }


    val reachableTypes = (queryRoot.fields ++ mutationRoot.fields)
      .flatMap(extractReachableTypes)
      .foldLeft[List[CaseClassType2]](Nil)((acc, next) => {
        ScalaType.unwrap(next) match {
          case c: CaseClassType2 => acc ++ List(c)
          case _ => acc
        }
      })
      .filterNot(tpe => globalTypeMap.keys.exists(_.contentEquals(tpe.name))) ++ args

    CalibanAPI(
      models = generatedTypes.flatMap(_.models),
      inputOutputTypes = reachableTypes.distinct,
      implicits = schemaImplicits ++ argbuilderImplicits,
      queryRoot = queryRoot,
      mutationRoot = mutationRoot,
      unreachableTypes = unreachableTypeMethods,
      entityResolvers = generatedTypes.flatMap(_.entityResolvers),
      extensions = extensionTrait,
      extensionArgTypes = extensionArgTypes,
      federationArgs = generatedTypes.flatMap(_.entityResolverKeys),
      tables = tables,
      objectExtensions = externalExtensions,
      mutationOps = generatedTypes.flatMap(v => v.mutationFields).map {
        case (_, t, opName, methodName) => (t, opName, methodName)
      }
    )
  }

  def isComposite(tpe: ScalaType): Boolean = false

  def extractReachableTypes(field2: Field2): List[ScalaType] = {
    List(field2.scalaType) ++ field2.arguments.map(_.tpe).flatMap(tpe => extractReachableTypes(tpe))
  }

  def extractReachableTypes(tpe: ScalaType): List[ScalaType] = {
    tpe match {
      case baseType: BaseType => List(baseType)
      case containerType: ContainerType => containerType match {
        case ScalaOption(inner) => extractReachableTypes(inner)
        case ScalaList(inner) => extractReachableTypes(inner)
        case AbstractEffect(inner) => extractReachableTypes(inner)
      }
      case c:  CaseClassType2 => List(c) ++ c.fields.flatMap(field => extractReachableTypes(field.scalaType))
      case TypeReference(tpe) => extractReachableTypes(tpe())
    }

  }

  def needsSchema(tpe: ScalaType): Boolean = {
    tpe match {
      case baseType: BaseType => false
      case containerType: ContainerType =>
        containerType match {
          case ScalaOption(inner)    => needsSchema(inner)
          case ScalaList(inner)      => needsSchema(inner)
          case AbstractEffect(inner) => needsSchema(inner)
        }
      case CaseClassType2(name, fields, annotations, extendsClauses) => true
      case TypeReference(tpe)                                        => needsSchema(tpe())
    }
  }

  def createEntityHandlerMethod(
      modelType: CaseClassType2
  ): GeneratedMethod = {
    val params =
      "extensions: ExtensionLogicMap, tables: List[Table], conditionsHandlerMap: Map[String, InputValue => String], connection: Connection"
    val impl = """new EntityResolver[Any] {
      |    override def resolve(value: InputValue): ZQuery[Any, CalibanError, Step[Any]] = genericEntityHandler(extensions, tables, conditionsHandlerMap, connection)(value)
      |    override def toType: __Type = schema.toType_()
      |}""".stripMargin

    GeneratedMethod(
      s"create${modelType.name}EntityResolver",
      params,
      s"implicit schema: Schema[Any,${modelType.name}]",
      "EntityResolver[Any]",
      impl
    )
  }

  def createRelationshipFields2(
      table: Whatever.Table,
      tables: List[Whatever.Table]
  ): List[Field2] = {
    println(s"CREATING RELATIONSHIPS FOR ${table.name}")
    //TODO: Disregard omitted tables
    val tableColumnTables = table.tableColumns

    table.relationships.map { link =>
      if (link.pkTableName == table.name) {
        // Some other table references this table
        // Options are -> Option["other-type"], List["other-type"]

        // "other-type" cant occur. We are the primary
        // Option["other-type"] can occur. Our reference is unique and their reference is unique
        // List["other-type"] can occur. Our reference is non-unique or their reference is non-unique
        val otherTableName =
          if (link.pkTableName == table.name) link.fkTableName
          else link.pkTableName

        val thisTableReferenceName =
          if (link.pkTableName == table.name) link.pkColumnName
          else link.fkColumnName
        val thatTableReferenceName =
          if (link.pkTableName == table.name) link.fkColumnName
          else link.pkColumnName

        val thisTableReferenceUnique = table.tableColumns
          .find(_.name == thisTableReferenceName)
          .map(_.unique)
          .getOrElse(
            throw new RuntimeException(
              s"Unable to find column ${thisTableReferenceName} mentioned in $link"
            )
          )
        val thatTable = tables
          .find(_.name == otherTableName)
          .getOrElse(
            throw new RuntimeException(
              s"Unable to find table $otherTableName mentioned in ${link}"
            )
          )
        val thatTableReferenceUnique =
          thatTable.tableColumns
            .find(_.name == thatTableReferenceName)
            .map(_.unique)
            .getOrElse(
              throw new RuntimeException(
                s"Unable to find column ${thatTableReferenceName} mentioned in $link"
              )
            )
        val (linkType, params) =
          if (thisTableReferenceUnique && thatTableReferenceUnique)
            (() => ScalaOption(globalTypeMap(otherTableName)), Nil)
          else {
            (
              () => ScalaList(globalTypeMap(otherTableName)),
              List(thatTable.toPaginationParams)
            )
          }
        val fieldName =
          if (tableColumnTables.exists(_.name.contentEquals(otherTableName)))
            s"${otherTableName}_ref"
          else otherTableName
        Field2(
          fieldName,
          AbstractEffect(TypeReference(linkType)),
          Nil,
          params
        )
      } else {
        // This table is foreign key, we are linking to some other table

        val otherTableName =
          if (link.pkTableName == table.name) link.fkTableName
          else link.pkTableName
        val thisTableReferenceName =
          if (link.pkTableName == table.name) link.pkColumnName
          else link.fkColumnName
        val thatTableReferenceName =
          if (link.pkTableName == table.name) link.fkColumnName
          else link.pkColumnName

        val thisTableReferenceNullable =
          table.tableColumns
            .find(_.name.contentEquals(thisTableReferenceName))
            .map(_.nullable)
            .getOrElse(
              throw new RuntimeException(
                s"Unable to find column ${thisTableReferenceName} in table ${table.name} mentioned in $link"
              )
            )
        val thatTable = tables
          .find(_.matchesName(otherTableName))
          .getOrElse(
            throw new RuntimeException(
              s"Unable to find table $otherTableName mentioned in ${link}"
            )
          )
        val thatTableReferenceUnique =
          thatTable.tableColumns
            .find(_.name.contentEquals(thatTableReferenceName))
            .map(_.unique)
            .getOrElse(
              throw new RuntimeException(
                s"Unable to find column ${thatTableReferenceName} in table ${table.name} mentioned in $link"
              )
            )

        // For "other-type" to occur our reference must be non-nullable and their reference must be unique
        // for Option["other-type"] to occur our reference must be nullable and their reference must be unique
        // for List["other-type"] to occur our reference must be nullable and their reference must not be unique
        // for NonEmptyList["other-type"] to occur our reference be non-nullable and their reference must not non unique
        // if this tables reference column is non-nullable AND that tables reference column is unique THEN "other-type"
        // if this tables reference column is nullable AND that tables reference column is unique THEN Option["other-type"]
        // if this tables reference column is nullable AND that tables reference column is not unique THEN List["other-type"]
        // if this tables reference column is non-nullable AND that tables reference column is not unique THEN NonEmptyList["other-type"]
        val otherTableModelType = () => globalTypeMap(otherTableName)
        val (linkType, params) =
          (thisTableReferenceNullable, thatTableReferenceUnique) match {
            case (true, true) =>
              (ScalaOption(TypeReference(otherTableModelType)), Nil)
            case (true, false) =>
              (
                ScalaList(TypeReference(otherTableModelType)),
                List(thatTable.toPaginationParams)
              )
            case (false, false) =>
              (
                ScalaList(TypeReference(otherTableModelType)),
                List(thatTable.toPaginationParams)
              ) // Could refine to non-empty list
            case (false, true) => (TypeReference(otherTableModelType), Nil)
          }
        val fieldName =
          if (tableColumnTables.exists(_.name.contentEquals(otherTableName)))
            s"${otherTableName}_ref"
          else otherTableName
        Field2(
          fieldName,
          AbstractEffect(linkType),
          Nil,
          params
        )
      }
    }
  }
}

object Util {

  def typeToScalaTypeV2(
      tpe: Type,
      alreadyWrapped: Boolean = false,
      external: Boolean
  ): Version2.ScalaType = {
    tpe match {
      case Type.NamedType(name, nonNull) =>
        val base = name match {
          case "String" => ScalaString
          case "Int"    => ScalaInt
          case "ID"     => if (external) ScalaString else ScalaUUID
          case other    => Version2.globalTypeMap(other)
        }
        if (!alreadyWrapped && !nonNull) {
          ScalaOption(base)
        } else {
          base
        }

      case Type.ListType(ofType, nonNull) =>
        ScalaList(
          typeToScalaTypeV2(ofType, alreadyWrapped = true, external = external)
        )
    }
  }

  def extractTypeName(tpe: __Type): String = {
    tpe.name match {
      case Some(value) => value
      case None =>
        tpe.ofType match {
          case Some(value) => extractTypeName(value)
          case None        => throw new RuntimeException("Should never happen")
        }
    }
  }

  def extractUnwrappedTypename(tpe: __Type): String = {
    tpe.kind match {
      case __TypeKind.SCALAR =>
        tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.OBJECT =>
        tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.INTERFACE =>
        tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.UNION =>
        tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.ENUM =>
        tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.INPUT_OBJECT =>
        tpe.name.getOrElse(throw new RuntimeException("Should never happen"))
      case __TypeKind.LIST =>
        extractUnwrappedTypename(
          tpe.ofType.getOrElse(
            throw new RuntimeException("Should never happen")
          )
        )
      case __TypeKind.NON_NULL =>
        extractUnwrappedTypename(
          tpe.ofType.getOrElse(
            throw new RuntimeException("Should never happen")
          )
        )
    }
  }

  def findTableFromFieldTypeV2(
      fieldType: __Type,
      tables: List[Whatever.Table]
  ): Whatever.Table = {
    val unwrappedTypename = extractUnwrappedTypename(fieldType)
    tables
      .find(_.matchesName(unwrappedTypename))
      .getOrElse(
        throw new RuntimeException(
          s"Unable to find table $unwrappedTypename from type"
        )
      )
  }

  def findTableFromFieldTypeReprQueryV2(
      field: CField,
      tables: List[Whatever.Table]
  ): Whatever.Table = {
    val tableName = field.fields.headOption
      .flatMap(_.parentType)
      .flatMap(_.name)
      .getOrElse(
        throw new RuntimeException(
          "Unable to find table name for _entities request"
        )
      )
    tables
      .find(_.matchesName(tableName))
      .getOrElse(
        throw new RuntimeException(s"Unable to find table $tableName from type")
      )
  }

  def fieldToSQLResponseType(
      fieldType: __Type,
      nullable: Boolean
  ): SQLResponseType = {
    fieldType.kind match {
      case __TypeKind.SCALAR       => ???
      case __TypeKind.OBJECT       => ObjectResponse
      case __TypeKind.INTERFACE    => ObjectResponse
      case __TypeKind.UNION        => ObjectResponse
      case __TypeKind.ENUM         => ???
      case __TypeKind.INPUT_OBJECT => ???
      case __TypeKind.LIST         => ListResponse
      case __TypeKind.NON_NULL =>
        fieldToSQLResponseType(
          fieldType.ofType.getOrElse(
            throw new RuntimeException("Should never happen")
          ),
          false
        )
    }
  }

  def matchesTypenameQuery(field: CField): Boolean = {
    field.name.contentEquals("__typename")
  }

  def toIntermediateV2v2(
      field: CField,
      tables: List[Whatever.Table],
      fieldName: String,
      extensions: List[ExtensionPosition],
      rootLevelOfEntityResolver: Boolean = false,
      whereClauses: List[String]
  ): IntermediateV2 = {
    // Find the table
    val table =
      if (rootLevelOfEntityResolver)
        findTableFromFieldTypeReprQueryV2(field, tables)
      else findTableFromFieldTypeV2(field.fieldType, tables)
    // Remove extension fields and __typename fields
    val sqlFields = field.fields
      .filterNot(matchesTypenameQuery)
      .filterNot(field => extensions.exists(_.matchesField(field, table.name)))
    // Find the columns we fetch from this table
    // Find the recursive cases
    val (tableFields, recursiveFields) = sqlFields
      .partition(field => table.tableColumns.exists(_.matchesName(field.name)))

    // Recurse on the recursive cases (joins)
    val joins = recursiveFields
      .map { recursiveField =>
        val otherTableName =
          findTableFromFieldTypeReprQueryV2(recursiveField, tables).name
        val (leftCol, rightCol) = table.relationships
          .find { relationshipTable =>
            relationshipTable.pkTableName == table.name && relationshipTable.fkTableName == otherTableName
          }
          .map(link => (link.pkColumnName, link.fkColumnName))
          .orElse(
            table.relationships
              .find { relationshipTable =>
                relationshipTable.fkTableName == table.name && relationshipTable.pkTableName == otherTableName
              }
              .map(link => (link.fkColumnName, link.pkColumnName))
          )
          .getOrElse(
            throw new RuntimeException(
              s"Unable to find link between ${table.name} and ${otherTableName} required by ${field}"
            )
          )

        val whereClause =
          List(s""""${fieldName}_data"."$leftCol" = "$rightCol" """)
        //"dataId"."joinleftcol" = "joinrightcol"
        (
          toIntermediateV2v2(
            recursiveField,
            tables,
            s"${fieldName}.${recursiveField.name}",
            extensions,
            false,
            whereClause
          ),
          recursiveField
        )
      }
      .map { case (otherTable, field) =>
        val otherTableName = otherTable.from.tableName

        // Find the link between the tables, it has to exist
        // First check if this table is the primary key table
        // Then check if this table is the foreign key table
        table.relationships
          .find { relationshipTable =>
            relationshipTable.pkTableName == table.name && relationshipTable.fkTableName == otherTableName
          }
          .map(link =>
            IntermediateJoinV2(
              link.pkColumnName,
              link.fkColumnName,
              otherTable,
              otherTableName
            )
          )
          .orElse(
            table.relationships
              .find { relationshipTable =>
                relationshipTable.fkTableName == table.name && relationshipTable.pkTableName == otherTableName
              }
              .map(link =>
                IntermediateJoinV2(
                  link.fkColumnName,
                  link.pkColumnName,
                  otherTable,
                  otherTableName
                )
              )
          )
          .getOrElse(
            throw new RuntimeException(
              s"Unable to find link between ${table.name} and ${otherTable} required by ${field}"
            )
          )
      }

    // Fetch any columns the extensions depend on
    val usedExtensions = table.objectExtensions.filter(extension =>
      field.fields.exists(_.name.contentEquals(extension.field.fieldName))
    )
    val requirementsFromExtensions =
      usedExtensions.flatMap(v => v.field.requiredFields)
    // Caliban federation handling is weird
    val responseType =
      if (rootLevelOfEntityResolver) ObjectResponse
      else fieldToSQLResponseType(field.fieldType, nullable = true)
    val ordering = None

    val requirementsColumns = requirementsFromExtensions.map(requiredField =>
      IntermediateColumn(requiredField, requiredField)
    )
    val tableColumns = tableFields
      .map(field => field -> table.tableColumns.find(_.matchesName(field.name)))
      .map { case (field, column) =>
        IntermediateColumn(
          column
            .getOrElse(throw new RuntimeException("Should never happen"))
            .name,
          field.alias.getOrElse(field.name)
        )
      }

    val pagination = responseType match {
      case ObjectResponse => None
      case ListResponse   => Option(table.extractPaginationSizing(field))
    }

    val whereClauseFromField = responseType match {
      case ObjectResponse =>
        table match {
          case prim: Whatever.PrimaryKeyTable =>
            Option(prim.extractSingleItemCondition(field))
          // This shouldn't happen for now, but in general needs work
          // Right now we only expose getById and list operations and only for primary key tables
          // In order for us to guarantee at most a single item one condition must be satisfied
          // - We are querying with a where clause on a column with a unique value
          // This is trivially the case with getById on a primary key table
          // However this is considerably more complicated if we start supporting expressions.
          // In general i guess we should always return lists for expressions, because we can't dependently type queries
          // I guess its possible to expose a list and a single item variant of search with expressions
          // the single item variant would then be restricted to only allow expressions that can produce a single item

          case _: Whatever.NonPrimaryKeyTable => None
        }
      case ListResponse => None
    }

    IntermediateV2Regular(
      requirementsColumns ++ tableColumns,
      IntermediateFromV2(table.name, joins),
      responseType,
      ordering,
      whereClauses ++ whereClauseFromField,
      pagination,
      fieldName
    )
  }

  def intermdiateV2ToSQL(
      intermediate: IntermediateV2
  ): String = {
    val dataId = intermediate.dataId

    val thisTableSQL = intermediate match {
      case reg: IntermediateV2Regular =>
        /*
        select "json_spec"
        from (
          select "$dataId"."colName" as "outputName",
          select "$dataId"."colName" as "outputName",
          select "$dataId"."colName" as "outputName",
        )
         */
        val tableCols = reg.cols.map { col =>
          s""""$dataId"."${col.nameInTable}" as "${col.outputName}""""
        }

        val joinCols = intermediate.from.joins.map { join =>
          val fieldName = join.name
          s""" "${intermediate.fieldName}.$fieldName"."${intermediate.fieldName}.$fieldName" as "$fieldName" """
        } ++ List(s"""'${reg.from.tableName}' as "__typename"""")

        val effectiveCondition =
          intermediate.where.filter(_.nonEmpty) match {
            case Nil  => ""
            case list => list.mkString(" where ", " AND ", "")
          }

        println(
          s"Condition: ${intermediate.where}. Effective: $effectiveCondition. $intermediate"
        )

        s"""
             |select row_to_json(
             |    (
             |      select "json_spec"
             |      from (
             |        select ${(tableCols ++ joinCols).mkString(", ")}
             |      ) as "json_spec"
             |     )
             |) as "${intermediate.fieldName}"
             |from (select * from ${reg.from.tableName} $effectiveCondition ${intermediate.pagination
          .getOrElse("")}) as "$dataId"
             |    """.stripMargin
      case union: IntermediateV2Union =>
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

        union.discriminatorMapping.size match {
          case 0 =>
            union.responseType match {
              case ObjectResponse => "select json_agg('null')"
              case ListResponse   => "select json_agg('[]')"
            }
          case 1 =>
            //TODO:
            "regular table"
          case _ =>
            val last = union.discriminatorMapping.last
            val nMinusOne = union.discriminatorMapping.filterNot {
              case (k, _) =>
                k.contentEquals(last._1)
            }

            val elseClause = last._2.map { col =>
              s"""'${col.outputName}', "$dataId"."${col.nameInTable}""""
            } ++ List(s"""'__typename', '${last._1}'""").mkString(
              s"""else
                 |  json_build_object(
                 |""".stripMargin,
              ",",
              ")"
            )

            val whenClauses = nMinusOne.map {
              case (discriminatorValue, columns) =>
                columns.map { col =>
                  s"""'${col.outputName}', "$dataId"."${col.nameInTable}""""
                } ++ List(s"""'__typename', '$discriminatorValue'""").mkString(
                  s"""when "$dataId"."$union.discriminatorFieldName" = '$discriminatorValue'
                   |  then json_build_object(
                   |""".stripMargin,
                  ",",
                  ")"
                )
            }

            val subSelect = union.responseType match {
              case ObjectResponse => " -> 0"
              case ListResponse   => ""
            }

            val effectiveCondition =
              (union.where.toList.filter(_.nonEmpty)) match {
                case Nil  => ""
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
                 |) $subSelect as ${intermediate.fieldName}
                 |from (select * from ${intermediate.from.tableName} $effectiveCondition ${union.pagination
              .getOrElse("")}) as "$dataId"
                 |""".stripMargin

        }
    }

    val joins = intermediate.from.joins.map { join =>
      (
        intermdiateV2ToSQL(
          join.right
        ),
        s"${intermediate.fieldName}.${join.right.from.tableName}"
      )
    }

    val thisTableWithJoins = joins
      .foldLeft(thisTableSQL)((acc, next) => {
        acc ++ s""" LEFT JOIN LATERAL (${next._1}) as "${next._2}" on true """
      })

    intermediate.responseType.wrapInErrorHandling(
      intermediate.fieldName,
      thisTableWithJoins
    )
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

  def compileOfType(tpe: __Type): List[String] = {
    tpe.ofType match {
      case Some(value) => tpe.name.toList ++ compileOfType(value)
      case None        => tpe.name.toList
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

  def extractFunctionColumns(
      resultSet: ResultSet
  )(implicit c: Connection): FunctionColumnInfo = {
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

  def extractFunctions(
      resultSet: ResultSet
  )(implicit c: Connection): FunctionInfo = {
    FunctionInfo(
      resultSet.getString("FUNCTION_NAME"),
      resultSet.getString("REMARKS"),
      resultSet.getShort("FUNCTION_TYPE")
    )
  }

  def extractProcedureColumns(
      resultSet: ResultSet
  )(implicit c: Connection): ProcedureColumnInfo = {
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

  def extractProcedures(
      resultSet: ResultSet
  )(implicit c: Connection): ProcedureInfo = {
    ProcedureInfo(
      resultSet.getString("PROCEDURE_NAME"),
      resultSet.getString("REMARKS")
    )
  }

  def extractPrimaryKey(
      resultSet: ResultSet
  )(implicit c: Connection): PrimaryKeyInfo = {
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
      case "NO"  => false
      case _     => true
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

  def toTablesV2(
      tableInfo: List[TableInfo],
      columns: List[ColumnInfo],
      primaryKeyInfo: List[PrimaryKeyInfo],
      exportedKeys: List[ExportedKeyInfo],
      importedKeys: List[ImportedKeyInfo],
      indexes: List[IndexInfo],
      extensions: List[Version2.ObjectExtension]
  ): List[Whatever.Table] = {
    tableInfo.map { table =>
      val tableIndexes = indexes.filter(_.tableName.contentEquals(table.name))
      val primaryKey =
        primaryKeyInfo.filter(v => v.tableName.contentEquals(table.name))
      val allColumns = columns
        .filter(_.tableName.contentEquals(table.name))
        .map(colInfo =>
          Column(
            colInfo.name,
            colInfo.nullable,
            colInfo.sqltype,
            colInfo.typeName,
            tableIndexes.exists(index =>
              index.columnName.contentEquals(colInfo.name) && !index.nonUnique
            )
          )
        )
      val (primaryKeyColumn, otherColumns) = allColumns.partition(column =>
        primaryKey.exists(_.columnName.contentEquals(column.name))
      )
      val links = exportedKeys.filter { key =>
        List(key.fkTableName, key.pkTableName).contains(table.name)
      }
      primaryKeyColumn match {
        case Nil =>
          Whatever.NonPrimaryKeyTable(
            table.name,
            otherColumns,
            table.comment,
            links,
            extensions.filter(_.objectName.contentEquals(table.name))
          )
        case ::(head, tail) =>
          Whatever.PrimaryKeyTable(
            table.name,
            NonEmptyChunk(head, tail: _*),
            otherColumns,
            table.comment,
            links,
            extensions.filter(_.objectName.contentEquals(table.name))
          )
      }
    }
  }

  def extractNameWithWrapper(tpe: __Type, wrapper: Option[String]): String = {
    tpe.kind match {
      case __TypeKind.SCALAR =>
        val base = tpe.name.get match {
          case "String"  => "String"
          case "Integer" => "Int"
          case "ID"      => "UUID"
        }
        wrapper.map(wrap => s"$wrap[$base]").getOrElse(base)
      case __TypeKind.OBJECT =>
        val base = extractTypeName(tpe)
        wrapper.map(wrap => s"$wrap[$base]").getOrElse(base)
      case __TypeKind.INTERFACE    => ???
      case __TypeKind.UNION        => ???
      case __TypeKind.ENUM         => ???
      case __TypeKind.INPUT_OBJECT => ???
      case __TypeKind.LIST =>
        extractNameWithWrapper(tpe.ofType.get, Option("List"))
      case __TypeKind.NON_NULL => extractNameWithWrapper(tpe.ofType.get, None)
    }
  }

  def graphqlTypeToScalaType(tpe: Type): Version2.ScalaType = {
    val inner = tpe match {
      case Type.NamedType(name, nonNull) =>
        name match {
          case "Int"    => ScalaInt
          case "String" => ScalaString
          case "ID"     => ScalaUUID
          case _        => globalTypeMap(name)
        }
      case Type.ListType(ofType, nonNull) =>
        ScalaList(graphqlTypeToScalaType(ofType))
    }
    if (tpe.nullable) {
      ScalaOption(inner)
    } else {
      inner
    }
  }

  def federatedGraphqlTypeToScalaType(tpe: Type): Version2.ScalaType = {
    val inner = tpe match {
      case Type.NamedType(name, nonNull) =>
        name match {
          case "Int"    => ScalaInt
          case "String" => ScalaString
          case "ID"     => ScalaUUID
          case _        => globalTypeMap(name)
        }
      case Type.ListType(ofType, nonNull) =>
        ScalaList(federatedGraphqlTypeToScalaType(ofType))
    }
    if (tpe.nullable) {
      ScalaOption(inner)
    } else {
      inner
    }
  }

  def connToTables2(
      extensions: List[Version2.ObjectExtension]
  )(implicit conn: Connection): List[Whatever.Table] = {
    println("GETTING TABEL")
    val tablesRs = conn.getMetaData.getTables(null, null, null, Array("TABLE"))
    val tableInfo =
      results(tablesRs)(extractTable).filterNot(_.name.startsWith("hdb"))
    println("GETTING COLUMNS")
    val columnRs = conn.getMetaData.getColumns(null, null, null, null)
    val columnInfo = results(columnRs)(extractColumn)
    println("GETTING PRIMARY KEY INFO")
    val primaryKeyInfo = tableInfo.flatMap { table =>
      val primaryKeyRs = conn.getMetaData.getPrimaryKeys(null, null, table.name)
      results(primaryKeyRs)(extractPrimaryKey)
    }
    println("GETTING INDEXS")
    val indexInfo = tableInfo.flatMap { table =>
      val indexRs =
        conn.getMetaData.getIndexInfo(null, null, table.name, false, true)
      val indexes = results(indexRs)(extractIndexInfo)
      indexes
    }
    println("GETTING FOREIGN KEYSK")
    val exportedKeyInfo = tableInfo.flatMap { table =>
      val exportRs = conn.getMetaData.getExportedKeys(null, null, table.name)
      results(exportRs)(extractExportedKeyinfo)
    }

    exportedKeyInfo.foreach(println)
    println("GETTING OTHER KEYS")
    val importedKeyInfo = tableInfo.flatMap { table =>
      val importRs = conn.getMetaData.getImportedKeys(null, null, table.name)
      results(importRs)(extractImportedKeyinfo)
    }
    println()
    importedKeyInfo.foreach(println)
    println()

    toTablesV2(
      tableInfo,
      columnInfo,
      primaryKeyInfo,
      exportedKeyInfo,
      importedKeyInfo,
      indexInfo,
      extensions
    )
  }

  def fieldTypeToTspeName(fieldType: __Type, acc: String): String = {
    fieldType.ofType match {
      case Some(value) =>
        fieldTypeToTspeName(value, s"$acc ${fieldType.kind.toString} of ")
      case None =>
        s"""$acc ${fieldType.kind.toString} named ${fieldType.name.getOrElse(
          "WTF"
        )}"""
    }
  }

  def checkListMatches(
      field: CField,
      extension: Version2.ObjectExtension
  ): Boolean = {

    val typeName = Util.extractTypeName(field.fieldType)

    val res = typeName.contentEquals(
      extension.objectName
    ) //&& field.fields.exists(_.name.contentEquals(extension.field.fieldName))
    println(s"CHECK MATCHES OBJECT $typeName = ${extension.objectName} -> $res")
    res
  }

  def parseExtensions(extensionFileLocation: java.io.File): String = {
    if (!extensionFileLocation.exists()) ""
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
      case definition: TypeSystemDefinition =>
        definition match {
          case definition: TypeDefinition =>
            definition match {
              case t: TypeDefinition.ObjectTypeDefinition =>
                Option(t)
              case _ => Nil
            }
          case _ => Nil
        }
      case _ => Nil
    }
  }

  def docToObjectExtension(doc: Document): List[Version2.ObjectExtension] = {
    val objectDefinitions = doc.definitions.flatMap {
      case definition: TypeSystemDefinition =>
        definition match {

          case definition: TypeDefinition =>
            definition match {
              case TypeDefinition.ObjectTypeDefinition(
                    description,
                    name,
                    implements,
                    directives,
                    fields
                  ) =>
                val objectExtensionsFields = fields.map { field =>
                  val requiredFields = field.directives
                    .filter(_.name == "requires")
                    .map(
                      _.arguments.values.head.toInputString
                        .replaceAllLiterally("\"", "")
                    )
                  val isExternla =
                    field.directives.exists(_.name.contentEquals("external"))
                  val tpe =
                    typeToScalaTypeV2(field.ofType, external = isExternla)
                  ObjectField(
                    fieldName = field.name,
                    tpe = tpe,
                    requiredFields = requiredFields,
                    isExternla
                  )
                }

                objectExtensionsFields.map { objectExtensionsField =>
                  Version2.ObjectExtension(name, objectExtensionsField)
                }
              case _ => Nil
            }
          case _ => Nil
        }
      case _ => Nil
    }
    objectDefinitions
  }

  def docToObjectExtensionV2(doc: Document): List[Version2.ObjectExtension] = {
    val objectDefinitions = doc.definitions.flatMap {
      case definition: TypeSystemDefinition =>
        definition match {
          case definition: TypeDefinition =>
            definition match {
              case obj: TypeDefinition.ObjectTypeDefinition =>
                val objectExtensionsFields = obj.fields.map { field =>
                  val requiredFields = field.directives
                    .filter(_.name == "requires")
                    .map(
                      _.arguments.values.head.toInputString
                        .replaceAllLiterally("\"", "")
                    )
                  val isExternla =
                    field.directives.exists(_.name.contentEquals("external"))
                  val tpe =
                    typeToScalaTypeV2(field.ofType, external = isExternla)
                  Version2.ObjectField(
                    fieldName = field.name,
                    tpe = tpe,
                    requiredFields = requiredFields,
                    isExternla
                  )
                }

                objectExtensionsFields.map { objectExtensionsField =>
                  Version2.ObjectExtension(obj.name, objectExtensionsField)
                }
              case _ => Nil
            }
        }
      case _ => Nil
    }
    objectDefinitions
  }

  def docToFederatedInstances(doc: Document): List[ExternalEntity] = {
    doc.definitions.flatMap {
      case extension: Definition.TypeSystemExtension =>
        extension match {
          case extension: TypeSystemExtension.TypeExtension =>
            extension match {
              case TypeExtension.ScalarTypeExtension(name, directives) => Nil
              case TypeExtension.ObjectTypeExtension(
                    name,
                    implements,
                    directives,
                    fields
                  ) =>
                val externalFields = fields.map { field =>
                  ExternalField(
                    field.name,
                    field.ofType,
                    field.directives.exists(_.name.contentEquals("external"))
                  )
                }
                val keys = directives
                  .find(_.name.contentEquals("key"))
                  .flatMap(
                    _.arguments
                      .get("fields")
                  )
                  .map(_.toInputString.replaceAllLiterally("\n", ""))
                  .toList
                  .flatMap(_.split(" "))

                List(ExternalEntity(name, externalFields, keys))
              case _ => Nil
            }
        }
      case _ => Nil
    }
  }

  def checkExtensionValid(
      extensions: List[Version2.ObjectExtension],
      baseApi: Document
  ): List[String] = {
    val objectsInBaseDoc = objectsInDocument(baseApi)
    extensions.flatMap { extension =>
      objectsInBaseDoc.find(_.name.contentEquals(extension.objectName)) match {
        case Some(baseObject) =>
          extension.field.requiredFields.flatMap { requiredField =>
            baseObject.fields.find(_.name.contentEquals(requiredField)) match {
              case Some(requiredFieldOnBaseObject) =>
                val extensionFieldNullable = extension.field.tpe match {
                  case containerType: ContainerType =>
                    containerType match {
                      case _: ScalaList   => false
                      case _: ScalaOption => true
                    }
                  case _ => false
                }
                if (
                  !extensionFieldNullable && requiredFieldOnBaseObject.ofType.nullable
                )
                  List(
                    s"Object ${baseObject.name} has been extended with field ${extension.field.fieldName} with a nonnullable type. This field requires ${requiredFieldOnBaseObject.name} which is nullable. This does not fly."
                  )
                else Nil
              case None =>
                List(
                  s"Obejct ${baseObject.name} does not have required field ${requiredField} needed for ${extension.field.fieldName}"
                )
            }
          }
        case None => List(s"Object ${extension.objectName} does not exist")
      }
    }
  }
}

object Codegen extends App {
  import PostgresSniffer._
  val _ = classOf[org.postgresql.Driver]
  val con_str = "jdbc:postgresql://0.0.0.0:5438/product"
  implicit val conn =
    DriverManager.getConnection(con_str, "postgres", "postgres")

  val fileLocation = File("") / "src" / "main" / "scala"
  val dir = File(fileLocation) / "generated"

  //dir.createDirectory()

  val outputFile = dir / "boilerplate.scala"

}


object Poker extends App {
  import PostgresSniffer._
  val _ = classOf[org.postgresql.Driver]
  val con_str = "jdbc:postgresql://0.0.0.0:5438/product"
  implicit val conn =
    DriverManager.getConnection(con_str, "postgres", "postgres")


  val metadata = conn.getMetaData

  val procedures =
    results(metadata.getProcedures(null, null, null))(extractProcedures)

  println(s"PROCEDURES: ${procedures.size}")

  val proceduresWithColumns = procedures.map { procedure =>
    procedure -> results(
      metadata.getProcedureColumns(null, null, procedure.name, null)
    )(extractProcedureColumns)
  }

  proceduresWithColumns.foreach { case (procedure, columns) =>
    println(s"Procedure: ${procedure.name}")
    columns.foreach { column =>
      println(
        s"  ${column.columnName} -> ${column.columnType} -> ${column.dataType}"
      )
    }
  }

  val functions =
    results(metadata.getFunctions(null, null, null))(extractFunctions)

  println(s"FUNCTIONS: ${functions.size}")

  val functionsWithColumns =
    functions.filter(_.name.contentEquals("user_extended_info")).map {
      function =>
        function -> results(
          metadata.getFunctionColumns(null, null, function.name, null)
        )(extractFunctionColumns)
    }

  functionsWithColumns.foreach { case (procedure, columns) =>
    println(s"Function: ${procedure.name}")
    columns.foreach { column =>
      println(
        s"  ${column.columnName} -> ${column.columnType} -> ${column.dataType} -> ${column.typeName}"
      )
    }
  }

}

object SimplePoker extends App {
  import PostgresSniffer._
  val _ = classOf[org.postgresql.Driver]
  val con_str = "jdbc:postgresql://0.0.0.0:5439/postgres"
  implicit val conn =
    DriverManager.getConnection(con_str, "postgres", "postgres")


  val metadata = conn.getMetaData


//  println(metadata.getTableTypes)
//  val res = metadata.getColumns(null, "public", null, null)

//  val zomg = results(res)(extractColumn)

  val res2 = metadata.getFunctions(null, "public", null)

  val functions = results(res2)(extractFunctions)


  val res3 = metadata.getFunctionColumns(null, "public", null, null)
  val functionColumns = results(res3)(extractFunctionColumns)


  functions.foreach(println)
  println()

  functionColumns.groupBy(_.functionName).foreach { case (fname, cols) =>
    println(fname)
    cols.foreach(col => println(s"   ${col}"))
  }


//
//  val res = metadata.getTableTypes

//  while(res.next()) {
//    (0 to res.getMetaData.getColumnCount).foreach(i => print(s"${res.getObject(i)} "))
//    println()
//    println(res.getString("TABLE_TYPE"))
//  }

}
