package apistuff

import caliban.{CalibanError, GraphQL, InputValue, ResponseValue, Value}
import caliban.InputValue.ObjectValue
import caliban.execution.Field
import caliban.federation.{EntityResolver, federate}
import caliban.introspection.adt.{__DeprecatedArgs, __Directive, __Type}
import caliban.parsing.adt.Type
import caliban.schema.Step.{MetadataFunctionStep, ObjectStep, QueryStep}
import caliban.schema.{Operation, PureStep, RootSchemaBuilder, Schema, Step}
import caliban.wrappers.Wrapper
import codegen.Util.{argToWhereClause, createExtensionModifications, extendType, toIntermediate, toIntermediateFromReprQuery, toQueryWithJson}
import codegen.{JsonPathPart, ObjectExtension, ObjectExtensionField, Table}
import io.circe.{Decoder, DecodingFailure, Encoder, Json, JsonObject}
import zio.ZIO
import io.circe.generic.auto._
import io.circe.syntax._
import sun.util.resources.cldr.zh.TimeZoneNames_zh
import zio.query.ZQuery
import generic.Util._

import java.sql.Connection
import java.util.UUID

trait InsertInput {
  def toInsert(): String
}

trait UpdateInput {
  def toUpdate(inputValue: InputValue): String
}

trait DeleteInput {
  def toDelete(): String
}

case class User(id: UUID, name: Option[String], email: String)
case class InsertUserArgs(name: Option[String], email: String) extends InsertInput {
  def toInsert(): String =  {
    s"insert into users(name, email) values ('$name', '$email')"
  }
}

case class UserPk(id: UUID)
case class UpdateUserArgs(pk: UserPk,set: SetUserFields) extends UpdateInput {
  override def toUpdate(inputValue: InputValue): String = {
    val fieldValues = List(
      List(codegen.ObjectPath("set"), codegen.ObjectPath("email")),
      List(codegen.ObjectPath("set"), codegen.ObjectPath("name"))
    )
      .filter(path => isKeySet(inputValue, path))
      .map(path => getValueAtKey(inputValue, path))

    val pkCondition = s"id = '${pk.id}'"
    s"update users set ${fieldValues.mkString(" AND ")} where $pkCondition"
  }
}
case class SetUserFields(name: Option[String], email: Option[String])


case class Product(id: UUID, name: String, description: Option[String])
case class Order(id: UUID, buyerId: UUID) {
  def toInsert(): String = ???
}
case class LineItem(id: UUID, orderId: UUID, productId: UUID, amount: Int)

case class Queries(
    getUserById: Field => UUID => ZIO[Any, Throwable, User]
    // And so forth
                  )
case class Mutations(
    insertUser: Field => InsertUserArgs => User
    // And so forth
                    )

trait OperationValidator {
  def getUserById(field: Field, id: UUID): Boolean
  def insertUser(field: Field, args: InsertUserArgs): Boolean
  // And so forth
}

trait BeforeHandler {
  def getUserById(field: Field, id: UUID): ZIO[Any, Nothing, Unit]
  def insertUser(field: Field, args: InsertUserArgs): ZIO[Any, Nothing, Unit]
  // And so forth
}

trait AfterHandler {
  def getUserById(field: Field, id: UUID, result: Option[User]): ZIO[Any, Nothing, Unit]
  def insertUser(field: Field, args: InsertUserArgs, result: Either[String, User]): ZIO[Any, Nothing, Unit]
  // And so forth
}

trait Extensions {
  def userAddress(field: Field, id: UUID): ZIO[Any, Throwable, String]
}

object Util {
  def isKeySet(inputValue: InputValue, path: List[codegen.ObjectPath]): Boolean = {
    path match {
      case ::(head, next) =>
        inputValue match {
          case ObjectValue(fields) =>
            fields.keys.exists(_.contentEquals(head.fieldName)) && isKeySet(fields(head.fieldName), next)
          case _ => false
        }
      case Nil => true
    }
  }

  def getValueAtKey(inputValue: InputValue, path: List[codegen.ObjectPath]): String =  {
    ???
  }

  // Generic methods go here
  def middle[A](f: (Field, A) => Boolean)(field: Field, json: Json)(implicit decoder: Decoder[A]): Boolean =  {
    json.as[A] match {
      case Left(_) => false
      case Right(value) => f(field, value)
    }
  }


  def middleZIOUnit[A](f: (Field, A) => ZIO[Any, Throwable, Unit])(field: Field, json: Json)(implicit decoder: Decoder[A]): ZIO[Any, Throwable, Unit] =  {
    json.as[A] match {
      case Left(e) => ZIO.fail(e)
      case Right(value) => f(field, value)
    }
  }

  def middleGeneric[A, C](f: (Field, A) => ZIO[Any, Throwable, C])(field: Field, json: Json)(implicit decoder: Decoder[A], encoder: Encoder[C]): ZIO[Any, Throwable, JsonObject] =  {
    json.as[A] match {
      case Left(e) => ZIO.fail(e)
      case Right(value) =>
        f(field, value).map { output =>
          JsonObject.apply(field.name -> output.asJson)
        }
    }
  }

  def middleGenericJsonObjet[A, C](f: (Field, A) => ZIO[Any, Throwable, C])(field: Field)( json: JsonObject)(implicit decoder: Decoder[A], encoder: Encoder[C]): ZIO[Any, Throwable, JsonObject] =  {
    json.asJson.as[A] match {
      case Left(e) => ZIO.fail(e)
      case Right(value) =>
        f(field, value).map { output =>
          JsonObject.apply(field.name -> output.asJson)
        }
    }
  }

  def middle2[A, B](f: (Field, A, B) => ZIO[Any, Throwable, Unit])(field: Field, inputJson: Json, outputJson: Json)(implicit inputDecoder: Decoder[A], outputDecoder: Decoder[B]): ZIO[Any, Throwable, Unit] = {
    ZIO.fromEither(inputJson.as[A]).flatMap { input =>
      ZIO.fromEither(outputJson.as[B]).flatMap { output =>
        f(field, input, output)
      }
    }
  }

  def whatever[A <: InsertInput](json: Json)(implicit decoder: Decoder[A]): String = {
    json.as[A] match {
      case Left(value) => ""
      case Right(value) => value.toInsert()
    }
  }

  def genericMutationHandler[R](
                                 tables: List[Table],
                                 extensions: List[ObjectExtension],
                                 connection: Connection,
                                 mutationToInsertReader: Map[String, Json => String],
                                 mutationToUpdateReader: Map[String, Json => String],
                                 mutationToDeleteReader: Map[String, Json => String]
                               )(field: Field): Step[R] = {

    // We have created an intermediate that has filtered out all the fields that have @requires
    val intermediate = toIntermediate(field, tables, "root", extensions)
    val fieldMask: List[Json => Json] = Nil // Transformations for removing the fields that were added to satisfy extensions


    // Three types
    val (mutationPart) = if(field.name.startsWith("update")) {
      mutationToUpdateReader(extractTypeName(field.fieldType))(InputValue.asJson)
    } else if(field.name.startsWith("insert")) {
      mutationToInsertReader(extractTypeName(field.fieldType))(InputValue.asJson)
    } else {
      mutationToDeleteReader(extractTypeName(field.fieldType))(InputValue.asJson)
    }

    val readPartOfQuery = toQueryWithJson(intermediate.copy(from = intermediate.from
      .copy(tableName = "mutation_result")), "root", None)

    val fullQuery = s"""
                       |with mutation_result as ($mutationPart returning *)
                       |
                       |$readPartOfQuery
                       |
                       |""".stripMargin


    // Compute the modifications
    val extensionModifications = createExtensionModifications(field, null)

    // Prepare the statement
    // Execute the query
    // Extract the result
    // Parse the json
    // Apply all the modifications from our extensions
    // Parse to ResponseValue and return

    val completedRequest =
      ZIO(connection.prepareStatement(fullQuery))
        .flatMap(prepared => ZIO(prepared.executeQuery()))
        .flatMap(rs => ZIO{
          rs.next()
          rs.getString("root")
        })
        .flatMap(resultString => ZIO.fromEither(io.circe.parser.parse(resultString)))
        // TODO: Run this step in parallel
        .flatMap(json => extensionModifications.foldLeft(ZIO.succeed(json)){ (acc, next) =>
          acc.flatMap(next(_))
        })
        .flatMap(json =>ZIO.fromEither(ResponseValue.circeDecoder.decodeJson(json)))
        .map(responseValue => PureStep(responseValue))

    QueryStep(ZQuery.fromEffect(completedRequest))
  }

  def genericQueryHandler[R](tables: List[Table], extensions: List[ObjectExtension], connection: Connection)(field: Field): Step[R] = {
    // We have created an intermediate that has filtered out all the fields that have @requires
    val intermediate = toIntermediate(field, tables, "root", extensions)
    val fieldMask: List[Json => Json] = Nil // Transformations for removing the fields that were added to satisfy extensions

    val condition = field.arguments.map { case (k, v) => argToWhereClause(k, v) }.mkString(" AND ")
    val query = toQueryWithJson(intermediate, "root", Option(condition))

    println(query)
    // Compute the modifications
    val extensionModifications = createExtensionModifications(field, null)

    // Prepare the statement
    // Execute the query
    // Extract the result
    // Parse the json
    // Apply all the modifications from our extensions
    // Parse to ResponseValue and return

    val completedRequest =
      ZIO(connection.prepareStatement(query))
        .flatMap(prepared => ZIO(prepared.executeQuery()))
        .flatMap(rs => ZIO{
          rs.next()
          rs.getString("root")
        })
        .flatMap(resultString => ZIO.fromEither(io.circe.parser.parse(resultString)))
        // TODO: Run this step in parallel
        .flatMap(json => extensionModifications.foldLeft(ZIO.succeed(json)){ (acc, next) =>
          acc.flatMap(next(_))
        })
        .flatMap(json =>ZIO.fromEither(ResponseValue.circeDecoder.decodeJson(json)))
        .map(responseValue => PureStep(responseValue))

    QueryStep(ZQuery.fromEffect(completedRequest))
  }


  def genericEntityHandler[R](extensions: Map[ObjectExtension, (Field,Json) => ZIO[Any, Throwable, JsonObject]], tables: List[Table], conditionsHandlers: Map[String,ObjectValue => String], connection: Connection)(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
    ZQuery.succeed(Step.MetadataFunctionStep(field => {
      // Convert to intermediate Representation
      val intermediate = toIntermediateFromReprQuery(field, tables, "_entities", extensions.keys.toList)
      // Extract the keys from the arguments
      val condition = value match {
        case o @ ObjectValue(fields) =>
          val tpeName = InputValue.circeEncoder.apply(fields("__typename")).as[String].getOrElse(throw new RuntimeException("Should never happen"))
          conditionsHandlers(tpeName)(o)
        case _ => throw new RuntimeException("should never happen")
      }
      // Convert to query
      val query = toQueryWithJson(intermediate, "root", Option(condition))
      println(query)

      // Compute the modifications
      val extensionModifications = createExtensionModifications(field, null)

      // Prepare the statement
      // Execute the query
      // Extract the result
      // Parse the json
      // Apply all the modifications from our extensions
      // Parse to ResponseValue and return

      val completedRequest =
        ZIO(connection.prepareStatement(query))
        .flatMap(prepared => ZIO(prepared.executeQuery()))
        .flatMap(rs => ZIO{
          rs.next()
          rs.getString("root")
        })
        .flatMap(resultString => ZIO.fromEither(io.circe.parser.parse(resultString)))
          // TODO: Run this step in parallel
        .flatMap(json => extensionModifications.foldLeft(ZIO.succeed(json)){ (acc, next) =>
          acc.flatMap(next(_))
        })
        .flatMap(json =>ZIO.fromEither(ResponseValue.circeDecoder.decodeJson(json)))
        .map(responseValue => PureStep(responseValue))

      QueryStep(ZQuery.fromEffect(completedRequest))
    }))
  }


  def constructQueryOperation[R, S](connection: Connection, extensions: List[ObjectExtension], tables: List[Table])(implicit schema: Schema[R,S]): Operation[R] = {
    val tpe = extendType(schema.toType_(), extensions)

    val queryFields = tpe
      .fields(__DeprecatedArgs(Some(true)))
      .getOrElse(Nil)
      .map(field => field.name -> MetadataFunctionStep(genericQueryHandler(tables, extensions, connection)))
      .toMap

    val resolver = ObjectStep(
      tpe.name.getOrElse(throw new RuntimeException("Should never happen")),
      queryFields
    )
    Operation(tpe, resolver)
  }

  def constructMutationOperation[R, S](
                                        connection: Connection,
                                        extensions: List[ObjectExtension],
                                        tables: List[Table],
                                        mutationToInsertReader: Map[String, Json => String],
                                        mutationToUpdateReader: Map[String, Json => String],
                                        mutationToDeleteReader: Map[String, Json => String])(implicit schema: Schema[R,S]): Operation[R] = {
    val tpe = extendType(schema.toType_(), extensions)

    val queryFields = tpe
      .fields(__DeprecatedArgs(Some(true)))
      .getOrElse(Nil)
      .map(field => field.name -> MetadataFunctionStep(genericMutationHandler(tables, extensions, connection, mutationToInsertReader, mutationToUpdateReader, mutationToDeleteReader)))
      .toMap

    val resolver = ObjectStep(
      tpe.name.getOrElse(throw new RuntimeException("Should never happen")),
      queryFields
    )
    Operation(tpe, resolver)
  }
}

object EntityResolvers {
  def createOrderEntityResolver(
                                 extensions: Map[ObjectExtension, (Field,Json) => ZIO[Any, Throwable, JsonObject]],
                                 tables: List[Table],
                                 conditionsHandler: Map[String, ObjectValue => String],
                                 connection: Connection)(implicit schema: Schema[Any, Order]) = new EntityResolver[Any] {
    override def resolve(value: InputValue): ZQuery[Any, CalibanError, Step[Any]] = Util.genericEntityHandler(extensions, tables, conditionsHandler, connection)(value)

    override def toType: __Type = extendType(schema.toType_(), null)
  }

}

class Api(
           validators: List[OperationValidator],
           connection: Connection,
           beforeHandler: BeforeHandler,
           afterHandler: AfterHandler,
           extensions: Extensions)
         (implicit val querySchema: Schema[Any, Queries], mutationSchema: Schema[Any, Mutations]) {
  import Util._
  // Generated list of tables
  val tables = List()

  val extensionLogicByType: Map[ObjectExtension, (Field,Json) => ZIO[Any, Throwable, JsonObject]] = Map(
    ObjectExtension("User", ObjectExtensionField("address", Type.NamedType("String", nonNull = true), Nil)) -> middleGeneric[UUID, String](extensions.userAddress)
  )


  val musmusk: Map[ObjectExtension, Field => JsonObject => ZIO[Any, Throwable, JsonObject]] = Map(
    ObjectExtension("User", ObjectExtensionField("address", Type.NamedType("String", nonNull = true), Nil)) -> middleGenericJsonObjet[UUID, String](extensions.userAddress)
  )

  val insertByFieldName: Map[String, Json => String] = Map(
    "insertUser" -> whatever[InsertUserArgs]
  )

  val updateByFieldName: Map[String, Json => String] = Map(
  )

  val deleteByFieldName: Map[String, Json => String] = Map(
  )

  val validatorByFieldName: Map[String, List[(Field, Json) => Boolean]] = Map(
    "getUserById" -> validators.map(valid =>middle[UUID](valid.getUserById) _),
    "insertUser" -> validators.map(valid =>middle[InsertUserArgs](valid.insertUser) _)
  )

  val beforeHandlerByFieldName: Map[String, (Field, Json) => ZIO[Any, Throwable, Unit]] = Map(
    "getUserById" -> middleZIOUnit(beforeHandler.getUserById),
    "insertUser" -> middleZIOUnit(beforeHandler.insertUser)
  )

  val afterHandlerByFieldName: Map[String, (Field, Json, Json) => ZIO[Any, Throwable, Unit]] = Map(
    "getUserById" -> middle2(afterHandler.getUserById),
    "insertUser" -> middle2(afterHandler.insertUser)
  )

  trait FederationArg {
    def toSelect: String
  }

  case class UserFederationArgs(id: UUID) extends FederationArg {
    def toSelect(): String = {
      s"id = '$id'"
    }
  }
//
//  def convertFederationArg[A <: FederationArg](objectValue: ObjectValue): String = {
//    import caliban.InputValue._
//    objectValue.asJson.as[A] match {
//      case Left(value) => ""
//      case Right(value) => value.toSelect
//    }
//  }

//  val entityResolverConditionByTypeName: Map[String, ObjectValue => String] = Map(
//    "User" -> convertFederationArg(List("id"))
//  )

  val f : Field = ???

  val what = InputValue.circeEncoder.apply(ObjectValue(f.arguments))

  val baseApi = new GraphQL[Any] {
    override protected val schemaBuilder: RootSchemaBuilder[Any] = RootSchemaBuilder(
      Option(
        constructQueryOperation[Any,Queries](
          connection,
          extensionLogicByType.keys.toList,
          tables
        )
      ),
      Option(
        constructMutationOperation[Any, Mutations](
          connection,
          extensionLogicByType.keys.toList,
          tables,
          null,
          null,
          null
        )
      ),
      None
    )
    override protected val wrappers: List[Wrapper[Any]] = Nil
    override protected val additionalDirectives: List[__Directive] = Nil
  }


  def createApi(
               ): GraphQL[Any] = {
    federate(
      baseApi,
      createOrderEntityResolver(extensionLogicByType, tables, null, connection)
    )
  }
}
