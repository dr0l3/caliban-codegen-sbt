package generic

import caliban.InputValue.ObjectValue
import caliban.{CalibanError, InputValue, ResponseValue, Value}
import caliban.execution.Field
import caliban.introspection.adt.{__DeprecatedArgs, __Field, __Type, __TypeKind}
import caliban.parsing.adt.{Directive, Type}
import caliban.schema.{Operation, PureStep, Schema, Step, Types}
import caliban.schema.Step.{MetadataFunctionStep, ObjectStep, QueryStep}
import codegen.{ArrayPath, ArrayPathWrapper, FieldType, JsonPathPart, ObejctPathWrapper, ObjectExtension, ObjectExtensionField, ObjectPath, PathWrapper, Table}
import codegen.Util.{argToWhereClause, extendType, extractUnwrappedTypename, intermdiateV2ToSQL, toIntermediate, toIntermediateFromReprQuery, toIntermediateV2, toQueryWithJson}
import io.circe.{Decoder, Encoder, Json, JsonObject}
import io.circe.generic.auto._
import io.circe.optics.JsonPath.root
import io.circe.syntax._
import zio.ZIO
import zio.query.ZQuery

import scala.collection.compat._
import java.sql.Connection
import scala.util.{Failure, Success, Try}


sealed trait JsonPathPart2
case class ArrayPath2(inner: Option[ObjectPath2]) extends JsonPathPart2 // Array paths with no inner will be treated as terminal
case class ObjectPath2(fieldName: String, typeTest: Option[String]) extends JsonPathPart2

trait FederationArg {
  def toSelect: String
}

trait InsertInput {
  def toInsert(): String
}

trait UpdateInput {
  def toUpdate(): String
}

trait DeleteInput {
  def toDelete(): String
}

object Util {
  case class ExtensionPosition(objectName: String, fieldName: String, requirements: List[String] = Nil){
    def matchesField(field: Field, tableName: String): Boolean = {
      tableName.contentEquals(objectName) &&
        field.name.contentEquals(fieldName)
    }
  }
  type ExtensionLogicMap = List[(ExtensionPosition,Field => JsonObject => ZIO[Any,Throwable,JsonObject])]
  type AppliedExtensionLogicMap = List[(ExtensionPosition,JsonObject => ZIO[Any,Throwable,JsonObject])]

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
    __Field(extension.fieldName, None, Nil, () => extension.fieldType.to__Type, false, None, None) // TODO FIX
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

  def middleGeneric[A, C](f: (Field, A) => ZIO[Any, Throwable, C])(field: Field)(json: Json)(implicit decoder: Decoder[A], encoder: Encoder[C]): ZIO[Any, Throwable, JsonObject] =  {
    json.as[A] match {
      case Left(e) => ZIO.fail(e)
      case Right(value) =>
        f(field, value).map { output =>
          JsonObject.apply(field.name -> output.asJson)
        }
    }
  }

  def constructTypeNameThingy(field: Field, acc: Option[JsonObject]): JsonObject = {
    val (typeNameField, recursive) = field.fields.partition(_.name.contentEquals("__typename"))
    val thisLevel = typeNameField.headOption.map{field =>
      JsonObject.apply(field.name -> Json.fromString(extractTypeName(field.fieldType)))
    }.getOrElse(JsonObject.empty)
    recursive.map { field =>
      JsonObject(field.name -> constructTypeNameThingy(field, Option(thisLevel)).asJson)
    }.foldLeft(thisLevel)((acc, nextt) =>acc.deepMerge(nextt))
  }

  def middleGenericJsonObject[A, C](f: (Field, A) => ZIO[Any, Throwable, C])(fieldName: String)(field: Field)(json: JsonObject)(implicit decoder: Decoder[A], encoder: Encoder[C]): ZIO[Any, Throwable, JsonObject] =  {
    json.asJson.as[A] match {
      case Left(e) =>
        ZIO.fail(e)
      case Right(value) =>
        f(field, value).map { output =>
          json.deepMerge(JsonObject.apply(fieldName -> output.asJson))
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

    def convertFederationArg[A <: FederationArg](objectValue: ObjectValue)(implicit decoder: Decoder[A]): String = {
      InputValue.circeEncoder.apply(objectValue).as[A] match {
        case Left(value) => ""
        case Right(value) => value.toSelect
      }
    }

  def convertInsertInput[A <: InsertInput](json: Json)(implicit decoder: Decoder[A]): String = {
    println(json)
    json.as[A] match {
      case Left(value) =>
        println("UNABLE TO READ INSERT INPUT")
        value.printStackTrace()
        ""
      case Right(value) => value.toInsert()
    }
  }

  def convertUpdateInput[A <: UpdateInput](json: Json)(implicit decoder: Decoder[A]): String = {
    json.as[A] match {
      case Left(value) => ""
      case Right(value) => value.toUpdate()
    }
  }

  def convertDeleteInput[A <: DeleteInput](json: Json)(implicit decoder: Decoder[A]): String = {
    json.as[A] match {
      case Left(value) => ""
      case Right(value) => value.toDelete()
    }
  }

  def createModification(transformer: JsonObject => ZIO[Any,Throwable,JsonObject], path: List[JsonPathPart]): Json => ZIO[Any,Throwable, Json] = {
    import zio.interop.catz._
    path.foldLeft[PathWrapper](ObejctPathWrapper(root)) { (acc, next) =>
      next match {
        case ArrayPath => acc match {
          case ObejctPathWrapper(path) => ArrayPathWrapper(path.each)
          case ArrayPathWrapper(path) => ArrayPathWrapper(path.each)
        }
        case ObjectPath(fieldName, typeTest) => acc match {
          case ObejctPathWrapper(path) =>
            ObejctPathWrapper(path.selectDynamic(fieldName))
          case ArrayPathWrapper(path) => ArrayPathWrapper(path.selectDynamic(fieldName))
        }
      }
    } match {
      case ObejctPathWrapper(path) => path.obj.modifyF(transformer)
      case ArrayPathWrapper(path) =>path.obj.modifyF(transformer)
    }
  }

  def checkObjectMatches(field: Field, extension: ExtensionPosition): Boolean = {

    val typeNames = extractImplementingTypenames(field.fieldType)

    val res = typeNames.exists(_.contentEquals(extension.objectName)) && field.fields.exists(_.name.contentEquals(extension.fieldName))
    println(s"CHECK MATCHES OBJECT $typeNames = ${extension.objectName} -> $res")
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

  // LIST => Has no name
  // NONNULL => Has no name
  // Object => Has name
  // Union => Has name

  def extractTypeName(tpe: __Type): String = {

    tpe.name match {
      case Some(value) => value
      case None => tpe.ofType match {
        case Some(value) =>
          extractTypeName(value)
        case None => throw new RuntimeException("Should never happen")
      }
    }
  }

  // List[A] => [A]
  // NONNULL[A] => [A]
  // Object called A => [A]
  // Union = A | B => [A,B]
  def extractImplementingTypenames(tpe: __Type): List[String] = {
    tpe.name match {
      // EIther Object or Scalar
      case Some(value) if tpe.kind != __TypeKind.UNION => List(value)
      case Some(_) => tpe.possibleTypes.getOrElse(Nil).flatMap(extractImplementingTypenames)
      case None => tpe.ofType match {
        case Some(value) if tpe.kind == __TypeKind.UNION =>
          value.possibleTypes.getOrElse(Nil)
            .flatMap(extractImplementingTypenames)
        case Some(value) =>
          extractImplementingTypenames(value)
        case None =>
          println("IMPOSSIBLE CASE HIT")
          throw new RuntimeException("Should never happen")
      }
    }
  }

  def kindWithStrippedNonNull(fieldType: __Type): __TypeKind = {
    fieldType.kind match {
      case __TypeKind.NON_NULL => fieldType.ofType match {
        case Some(value) => kindWithStrippedNonNull(value)
        case None => throw new RuntimeException("NonNull has no oftype: Should never happen")
      }
      case _ => fieldType.kind
    }
  }

  def checkIfExtensionUsed(field: Field, extension: ExtensionPosition, currentPath: List[JsonPathPart]): List[List[JsonPathPart]] = {
    println(s"CHECK EXTENSION USED: $extension ${field.fieldType.kind}. ${field.fieldType.name}")
    kindWithStrippedNonNull(field.fieldType) match {
      case __TypeKind.SCALAR => Nil
      case __TypeKind.OBJECT =>
        println("OBJECT DETECTED")
        if(checkObjectMatches(field, extension)) { // TODO: FIX
          println(s"EXTENSION $extension USED IN OBJECT AT ${field.name}. PATH $currentPath")
          List(currentPath)
        } else {
          field.fields
            .flatMap(field => checkIfExtensionUsed(field, extension, currentPath ++ List(ObjectPath(field.name, None))))
        }
      case __TypeKind.INTERFACE => Nil
      case __TypeKind.UNION =>
        println("UNION DETECTED")
        // if possible types include the extension type
        // Then add the path with a type check
        val possibleTypes = field.fieldType.possibleTypes.getOrElse(Nil)
        println(s"POSSIBLETYPES: ${possibleTypes.flatMap(_.name)}")
        if(possibleTypes.flatMap(_.name).exists(_.contentEquals(extension.objectName))) {
          val currentWithTypeTest = currentPath.last match {
            case ArrayPath => ArrayPath
            case o: ObjectPath => o.copy(typeTest = Option(extension.objectName))
          }
          val modifiedCurrentPath = currentWithTypeTest :: currentPath.reverse.tail
          List(modifiedCurrentPath) ++ field.fields.flatMap(field => checkIfExtensionUsed(field, extension, modifiedCurrentPath ++ List(ObjectPath(field.name, None)) ))
        } else {
          field.fields.flatMap(field => checkIfExtensionUsed(field, extension, currentPath ++ List(ObjectPath(field.name, None)) ))
        }

      case __TypeKind.ENUM => Nil
      case __TypeKind.INPUT_OBJECT => Nil
      case __TypeKind.LIST =>
        println("LIST DETECTED")
        if(checkObjectMatches(field, extension)) {
          println(s"EXTENSION $extension USED IN LIST AT ${field.name}. PATH $currentPath")
          List(currentPath ++ List(ArrayPath))
        } else {
          println("RECURSING FROM LIST")
          field.fields
            .flatMap(field => checkIfExtensionUsed(field, extension, currentPath ++ List(ArrayPath, ObjectPath(field.name, None))))
        }
      case __TypeKind.NON_NULL =>
        // Will never happend ue to stripped non null
        Nil
    }

  }

//  def checkIfExtensionUsed2(field: Field, extension: ExtensionPosition, currentPath: List[JsonPathPart2]): List[List[JsonPathPart2]] = {
//    println(s"CHECK EXTENSION USED: $extension ${field.fieldType.kind}. ${field.fieldType.name}")
//    kindWithStrippedNonNull(field.fieldType) match {
//      case __TypeKind.SCALAR => Nil
//      case __TypeKind.OBJECT =>
//        println("OBJECT DETECTED")
//        if(checkObjectMatches(field, extension)) { // TODO: FIX
//          println(s"EXTENSION $extension USED IN OBJECT AT ${field.name}. PATH $currentPath")
//          List(currentPath) ++ field.fields
//            .flatMap(field => checkIfExtensionUsed2(field, extension, currentPath ++ List(ObjectPath2(field.name, None))))
//        } else {
//          field.fields
//            .flatMap(field => checkIfExtensionUsed2(field, extension, currentPath ++ List(ObjectPath2(field.name, None))))
//        }
//      case __TypeKind.INTERFACE => Nil
//      case __TypeKind.UNION =>
//        // if possible types include the extension type
//        // Then add the path with a type check
//        val possibleTypes = field.fieldType.possibleTypes.getOrElse(Nil)
//        if(possibleTypes.flatMap(_.name).exists(_.contentEquals(extension.objectName))) {
//          val currentWithTypeTest = currentPath.last match {
//            case a: ArrayPath2 => a.copy(inner = a.inner.map(_.copy(typeTest = Option(extension.objectName))))
//            case o: ObjectPath2 => o.copy(typeTest = Option(extension.objectName))
//          }
//          val modifiedCurrentPath = currentWithTypeTest :: currentPath.reverse.tail
//          modifiedCurrentPath ++ field.fields.flatMap(field => checkIfExtensionUsed2(field, extension, modifiedCurrentPath ++ List(ObjectPath2(field.name, None)) ))
//          Nil
//        } else {
//          field.fields.flatMap(field => checkIfExtensionUsed2(field, extension, currentPath ++ List(ObjectPath2(field.name, None)) ))
//        }
//
//      case __TypeKind.ENUM => Nil
//      case __TypeKind.INPUT_OBJECT => Nil
//      case __TypeKind.LIST =>
//        println("LIST DETECTED")
//        // We are looking at the list and the inner object matches, return current path and recurse
//        if(checkObjectMatches(field, extension)) {
//          println(s"EXTENSION $extension USED IN LIST AT ${field.name}. PATH $currentPath")
//          List(currentPath ++ List(ArrayPath2(None))) ++ field.fields
//            .flatMap(field => checkIfExtensionUsed2(field, extension, currentPath ++ List(ArrayPath2(Option(ObjectPath2(field.name, None))))))
//        } else {
//          println("RECURSING FROM LIST")
//          // We are looking at a list, but the inner object does not match, recurse
//          field.fields
//            .flatMap(field => checkIfExtensionUsed2(field, extension, currentPath ++ List(ArrayPath2(Option(ObjectPath2(field.name, None))))))
//        }
//      case __TypeKind.NON_NULL =>
//        // Will never happend due to stripped non null
//        Nil
//    }
//
//  }

  def createExtensionModifications(isEntityResolverQuery: Boolean, field: Field, extensions: Map[ExtensionPosition, JsonObject => ZIO[Any,Throwable,JsonObject]]): List[Json => ZIO[Any, Throwable,Json]] = {
    extensions.flatMap { case (extension, transformer) =>
      checkIfExtensionUsed(field, extension, Nil).map(path => path -> transformer)
    }.map { case (path, transformer) =>
      println(s"PATH: $path has transformer")
      createModification(transformer, if(isEntityResolverQuery) path.tail else path) // Shitty workaround for Caliban weirdness
    }.toList
  }

//  def createExtensionModifications2(isEntityResolverQuery: Boolean, field: Field, extensions: Map[ExtensionPosition, JsonObject => ZIO[Any,Throwable,JsonObject]]): List[(List[JsonPathPart2], JsonObject => ZIO[Any, Throwable,JsonObject])] = {
//    extensions.flatMap { case (extension, transformer) =>
//      checkIfExtensionUsed2(field, extension, Nil).map(path => path -> transformer)
//    }.toList
//  }


  def applyTransformationIfMatches(path: List[JsonPathPart], extensionTransformation: JsonObject => ZIO[Any, Throwable, JsonObject], json: Json): ZIO[Any, Throwable, Json] = {
    path match {
      case Nil =>
        json.asObject
          .map(obj => extensionTransformation(obj).map(_.asJson))
          .getOrElse(ZIO(json))
      case ::(head, tl) => head match {
        case ArrayPath =>
          ZIO
            .collectAll(json
              .asArray
              .getOrElse(Nil).toList
              .map(applyTransformationIfMatches(tl, extensionTransformation, _))
            )
            .map(arr => Json.arr(arr:_*))
        case ObjectPath(fieldName, typeTest) =>
          json.asObject.map{ obj =>
            obj(fieldName) match {
              case Some(jsonAtFieldName) =>
                typeTest match {
                  case Some(requiredTypename) =>
                    if(obj("__typename").flatMap(_.asString).exists(_.contentEquals(requiredTypename)))
                      applyTransformationIfMatches(tl, extensionTransformation, jsonAtFieldName)
                        .map(modifiedJson => obj.add(fieldName, modifiedJson).asJson)
                    else
                      ZIO(json)
                  case None =>

                    applyTransformationIfMatches(tl, extensionTransformation, jsonAtFieldName)
                      .map(modifiedJson => obj.add(fieldName, modifiedJson).asJson)
                }
              case None => ZIO(json)
            }
          }.getOrElse(ZIO(json))
      }
    }
  }

  def applyExtensions2(field: Field,extensionLogicMap: AppliedExtensionLogicMap, json: Json, entityQuery: Boolean): ZIO[Any, Throwable, Json] = {
    extensionLogicMap.flatMap { case (extension, transformer) =>
      checkIfExtensionUsed(field, extension, Nil).map(path => path -> transformer)
    }.foldLeft(ZIO(json)) { case(acc,(path, transformer))  =>
      acc.flatMap { json =>
        // Calibans weird handling of entity resolvers
        val effectivePath = if(entityQuery)path.tail else path
        println(effectivePath)
        applyTransformationIfMatches(effectivePath, transformer, json)
      }
    }
  }

  def passesTypeTest(): Boolean = ???

  def applySingleExtension(json: Json, extension: (List[JsonPathPart2], JsonObject => ZIO[Any,Throwable, JsonObject])): ZIO[Any, Throwable, Json] =  {
    import zio.interop.catz._
    val (path, extensionLogic) = extension
    path match {
      // There is no more path, we should apply the extension
      case Nil => root.obj.modifyF(extensionLogic)(json)
      // There is more path to follow
      case ::(head, tl) => head match {
        case ArrayPath2(inner) =>
          // We recurse with each object individually
          // We could make this more efficient by doing
          val individualObject =root.each
            .filter{candidate =>
              inner.flatMap(_.typeTest) match {
                case Some(value) =>
                  root.__typename.as[String].getOption(json).exists(_.contentEquals(value))
                case None => true
              }
            }
          ???
        case ObjectPath2(fieldName, typeTest) =>
          typeTest match {
            // A type test exists
            case Some(value) =>
              val typeInfo = root.__typename.as[String].getOption(json).getOrElse(throw new RuntimeException("Object always has __typename: Should never happne"))
              if(typeInfo.contentEquals(value)) {
                // Type test maches, we can recurse provided the value exists on the json object
                root.selectDynamic(fieldName).json.getOption(json) match {
                  // The value exists, recurse
                  case Some(value) => applySingleExtension(value, (tl, extensionLogic))
                  // Value does not exist, we can return the current json
                  case None => ZIO(json)
                }
              } else {
                // Type test failed, we should not apply the extension, we can return the current json
                ZIO(json)
              }
            // NO typetest
            case None =>
              // We recurse if the json exists
              root.selectDynamic(fieldName).json.getOption(json) match {
                case Some(value) => applySingleExtension(value,(tl, extensionLogic))
                case None => ZIO(json)
              }
          }
      }
    }
  }

  def applyAllExtensions(json: Json,field: Field, extensions: List[(List[JsonPathPart2],JsonObject => ZIO[Any, Throwable,JsonObject])]): ZIO[Any, Throwable,Json] = {
    // For each coordinate
    extensions.foldLeft(ZIO(json)) { case (acc, (path, transformer)) =>
      for {
        json <- acc
        afterExtension <- applySingleExtension(json, (path, transformer))
      } yield afterExtension
    }
  }

  def inputValueToPostgresExpression(value: InputValue): String = {
    value match {
      case InputValue.ListValue(values) => values.map(v => inputValueToPostgresExpression(v)).mkString("[", ",", "]")
      case ObjectValue(fields) => "" // TODO
      case InputValue.VariableValue(name) => "" // TODO
      case value: Value => value match {
        case Value.NullValue => "null"
        case value: Value.IntValue => value.toString
        case value: Value.FloatValue => value.toString
        case Value.StringValue(value) => s"'$value'"
        case Value.BooleanValue(value) => value.toString
        case Value.EnumValue(value) => value
      }
    }
  }

  def inputValueToInsertStatement(args: Map[String, InputValue], tableName: String): String = {
    val values = args.map { case (k, v) =>
      inputValueToPostgresExpression(v)
    }

    val keys = args.keys

    s"insert into $tableName(${keys.mkString(",")}) values (${values.mkString(",")})"
  }

  def inputValueToUpdateStatement(args: Map[String, InputValue], tableName: String): String = {
    val updates = args("set").toString
    val where = args("pk").toString
    s"update $tableName set $updates where $where"
  }

  def genericMutationHandler[R](
                                 tables: List[Table],
                                 extensions: ExtensionLogicMap,
                                 connection: Connection,
                                 mutationToInsertReader: Map[String, Json => String],
                                 mutationToUpdateReader: Map[String, Json => String],
                                 mutationToDeleteReader: Map[String, Json => String]
                               )(field: Field): Step[R] = QueryStep(ZQuery.fromEffect(ZIO{

    // We have created an intermediate that has filtered out all the fields that have @requires
    val intermediate = toIntermediate(field, tables, "root", extensions.map(_._1).toList)


    // Three types
    val (mutationPart) = if(field.name.startsWith("update")) {
      mutationToUpdateReader(field.name)(field.arguments.asJson)
    } else if(field.name.startsWith("insert")) {
      inputValueToInsertStatement(field.arguments, extractTypeName(field.fieldType))
    } else {
      mutationToDeleteReader(field.name)(field.arguments.asJson)
    }

    val readPartOfQuery = toQueryWithJson(intermediate.copy(from = intermediate.from
      .copy(tableName = "mutation_result")), "root", None)

    val fullQuery = s"""
                       |with mutation_result as ($mutationPart returning *)
                       |
                       |$readPartOfQuery
                       |
                       |""".stripMargin

    val coordinatesForTypenameQueries = checkForTypenameQuery(field, Nil).map {case (path, tpe) => path -> tpe}
    val cooridnatesDebug = coordinatesForTypenameQueries.map { case (path, typename) => s"${path} -> $typename"}.mkString("\n")


    val stuff =
      s"""|DEBUGGING
          |${printQuery(field)}
          |$cooridnatesDebug""".stripMargin
    val typeNameInsertions = constructTypenameInserters(coordinatesForTypenameQueries)

    // Prepare the statement
    // Execute the query
    // Extract the result
    // Parse the json
    // Apply all the modifications from our extensions
    // Parse to ResponseValue and return

    for {
      preparedStatement <- ZIO(connection.prepareStatement(fullQuery))
      resultSet <-ZIO(preparedStatement.executeQuery())
      resultString <- ZIO{
        resultSet.next()
        resultSet.getString("root")
      }
      resultAsJson <- ZIO.fromEither(io.circe.parser.parse(resultString))
      resultWithExtensions <- applyExtensions2(field,extensions.map{case (k,v) => k -> v.apply(field)}, resultAsJson, false)
      resultWithTypeNames = typeNameInsertions.foldLeft(resultWithExtensions)((acc, next) => next(acc))
      resultStripped = stripUnwantedStuff(resultWithTypeNames, field)
      resultAsResponseValue <- ZIO.fromEither(ResponseValue.circeDecoder.decodeJson(resultStripped))
      debugPrint =
        s"""
           |----------------------------------------
           |"Database query"
           |$fullQuery
           |QueryDebug
           |$stuff
           |DatabaseResult
           |$resultAsJson
           |DatabaseResult with extensions
           |$resultWithExtensions
           |DatabaseResult with extensions and typename
           |$resultWithTypeNames
           |ResultStripped
           |$resultStripped
           |--------------------------------------
           |""".stripMargin
      _ <- ZIO(println(debugPrint))
    } yield PureStep(resultAsResponseValue)
  }.flatten.tapError(e => ZIO(e.printStackTrace()))))

  def genericQueryHandler[R](tables: List[Table], extensions: ExtensionLogicMap, connection: Connection)(field: Field): Step[R] = QueryStep(ZQuery.fromEffect(ZIO{
    val start = System.nanoTime()
    // We have created an intermediate that has filtered out all the fields that have @requires
    val intermediate = toIntermediateV2(field, tables, "root", extensions.map(_._1))

    val condition = field.arguments.map { case (k, v) => argToWhereClause(k, v) }.mkString(" AND ")
    val query = intermdiateV2ToSQL(intermediate, "root", Option(condition))

    printTime(start, "QUery compilation")
    println(query)
    val coordinatesForTypenameQueries = checkForTypenameQuery(field, Nil).map {case (path, tpe) => path -> tpe}
    val cooridnatesDebug = coordinatesForTypenameQueries.map { case (path, typename) => s"${path} -> $typename"}.mkString("\n")


    val stuff =
      s"""|DEBUGGING
          |${printQuery(field)}
          |$cooridnatesDebug""".stripMargin
    val typeNameInsertions = constructTypenameInserters(coordinatesForTypenameQueries)

    // Prepare the statement
    // Execute the query
    // Extract the result
    // Parse the json
    // Apply all the modifications from our extensions
    // Parse to ResponseValue and return

    for {
      preparedStatement <- ZIO(connection.prepareStatement(query))
      _ = printTime(start, "Prepared statemnt")
      resultSet <-ZIO(preparedStatement.executeQuery())
      _ = printTime(start, "Query executuion")
      resultString <- ZIO{
        resultSet.next()
        resultSet.getString("root")
      }
      resultAsJson <- ZIO.fromEither(io.circe.parser.parse(resultString))
      _ = printTime(start, "Parsed result")
      resultWithExtensions <- applyExtensions2(field,extensions.map{case (k,v) => k -> v.apply(field)}, resultAsJson, false)
      _ = printTime(start, "Extensions")
      resultWithTypeNames = typeNameInsertions.foldLeft(resultWithExtensions)((acc, next) => next(acc))
      resultStripped = stripUnwantedStuff(resultWithTypeNames, field)
      _ = printTime(start, "Stripepd")
      resultAsResponseValue <- ZIO.fromEither(ResponseValue.circeDecoder.decodeJson(resultStripped))
      debugPrint =
        s"""
           |----------------------------------------
           |"Database query"
           |$query
           |QueryDebug
           |$stuff
           |DatabaseResult
           |$resultAsJson
           |DatabaseResult with extensions
           |$resultWithExtensions
           |DatabaseResult with extensions and typename
           |$resultWithTypeNames
           |ResultStripped
           |$resultStripped
           |--------------------------------------
           |""".stripMargin
      _ <- ZIO(println(debugPrint))
    } yield PureStep(resultAsResponseValue)

  }.flatten.tapError(e => ZIO(e.printStackTrace()))))

  def checkForTypenameQuery(field: Field, acc: List[JsonPathPart]): List[(List[JsonPathPart], String)] = {
    field.fieldType.kind match {
      case __TypeKind.SCALAR =>
        // Not relevant
        Nil
      case __TypeKind.OBJECT =>
        val (typeQuery, recursiveCases) = field.fields.partition(_.name.contentEquals("__typename"))
        typeQuery.map(_ => (acc, extractTypeName(field.fieldType))) ++ recursiveCases.flatMap (field => checkForTypenameQuery(field, acc ++ List(ObjectPath(field.name, None))))
      case __TypeKind.INTERFACE => Nil
      case __TypeKind.UNION => Nil
      case __TypeKind.ENUM =>
        // Not relevant
        Nil
      case __TypeKind.INPUT_OBJECT =>
        // Cant happen
        Nil
      case __TypeKind.LIST =>
        val (typeQuery, recursiveCases) = field.fields.partition(_.name.contentEquals("__typename"))
        typeQuery.map(_ => (acc, extractTypeName(field.fieldType))) ++ recursiveCases.flatMap(field => checkForTypenameQuery(field, acc ++ List(ArrayPath, ObjectPath(field.name, None))))
      case __TypeKind.NON_NULL =>
        val isArrayIncluded = includesArray(field.fieldType)

        val (typeQuery, recursiveCases) = field.fields.partition(_.name.contentEquals("__typename"))
        typeQuery.map(_ => (acc, extractTypeName(field.fieldType))) ++ recursiveCases.flatMap { field =>
          checkForTypenameQuery(field, if(isArrayIncluded) acc ++ List(ArrayPath, ObjectPath(field.name, None)) else acc ++ List(ObjectPath(field.name, None)))
        }
    }
  }

  // TODO: Rework this
  // Concept is flawed. We compute a typename from the field. This doesn't work with union types.
  // Either way it would be easier to just walk the resulting json rather than constructing elobarate methods
  def constructTypenameInserters(coordinates: List[(List[JsonPathPart], String)]): List[Json => Json] = {
    coordinates.map{ case (path, tpeName) =>
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
        case ObejctPathWrapper(path) => path.obj.modify(obj => if(obj.apply("__typename").nonEmpty) obj else obj.add("__typename", Json.fromString(tpeName)))
        case ArrayPathWrapper(path) =>path.obj.modify(obj => if(obj.apply("__typename").nonEmpty) obj else obj.add("__typename", Json.fromString(tpeName)))
      }
    }
  }

  def effectiveKind(tpe: __Type): __TypeKind = {
    tpe.kind match {
      case __TypeKind.NON_NULL => tpe.ofType.map(effectiveKind).getOrElse(throw new RuntimeException("NONNULL has no nested type: Should never happne"))
      case other => other
    }
  }

  def stripUnwantedStuff(json: Json, field: Field): Json = {
    println(s"STRIP UNWANTED STUFF(${field.name}, ${effectiveKind(field.fieldType)}) -> ${json}")
    field.fieldType.kind match {
      case __TypeKind.SCALAR => json
      case __TypeKind.OBJECT =>
        json.asObject match {
          case Some(value) =>
            val requestedFieldsOnThisLevel = field.fields
            value.keys
              .map(key => key -> requestedFieldsOnThisLevel.find(field => field.alias.getOrElse(field.name).contentEquals(key)))
              .foldLeft(value) { case (json, (key, fieldForKey)) =>
                fieldForKey match {
                  case Some(recursiveField) =>
                    val strippedField = stripUnwantedStuff(json(key).get, recursiveField)
                    val after = json.remove(key).add(key, strippedField)
                    println(s"KEEPING $key and recursing. AFTER: $after. Before ${json(key).get}. StripptedField: ${strippedField}")
                    after

                  case None =>

                    val after = json.remove(key)
                    println(s"REMOVING $key from $json AFTER: ${after}")
                    after
                }
            }.asJson

          case None => json
        }
      case __TypeKind.INTERFACE => json
      case __TypeKind.UNION =>
        json.asObject match {
          case Some(value) =>
            val typeName = value.apply("__typename").get.asString.get // TODO: FIX
            val requestedFieldsOnThisLevel = field.fields.filter(field => field.parentType.flatMap(_.name).exists(_.contentEquals(typeName)) || field.name.contentEquals("__typename")) // TODO: FIX
            println(s"REQUESTED FIELDS ${field.fields.map(_.name)}")
            println(s"REQUESTED FOR OBJET $typeName:  ${requestedFieldsOnThisLevel.map(_.name)}")
            value.keys
              .map(key => key -> requestedFieldsOnThisLevel.find(field => field.alias.getOrElse(field.name).contentEquals(key)))
              .foldLeft(value) { case (json, (key, fieldForKey)) =>
                fieldForKey match {
                  case Some(recursiveField) =>
                    val strippedField = stripUnwantedStuff(json(key).get, recursiveField) //TODO: FIX
                    val after = json.remove(key).add(key, strippedField)
                    println(s"KEEPING $key and recursing. AFTER: $after. Before ${json(key).get}. StripptedField: ${strippedField}")
                    after

                  case None =>

                    val after = json.remove(key)
                    println(s"REMOVING $key from $json AFTER: ${after}")
                    after
                }
              }.asJson

          case None => json
        }
      case __TypeKind.ENUM => json
      case __TypeKind.INPUT_OBJECT => json
      case __TypeKind.LIST =>
        val withPatchedKind = field.copy(fieldType = field.fieldType.ofType.getOrElse(throw new RuntimeException("List has no inner type: Should never happen")))
        json.asArray.getOrElse(Nil).map(json => stripUnwantedStuff(json,withPatchedKind)).toList.asJson
      case __TypeKind.NON_NULL =>
        stripUnwantedStuff(json, field.copy(fieldType = field.fieldType.ofType.getOrElse(throw new RuntimeException("NON_NULL has no inner type: Should never happen"))))
    }
  }


  def printQuery(field: Field, indent: String = ""): String = {
    s"""|$indent${field.name}
       |${field.fields.map(printQuery(_, s"$indent  ")).mkString("")}""".stripMargin
  }

  def printTime(start: Long, message: String): Unit = {
    import scala.concurrent.duration._
    val now = System.nanoTime()

    val timeElapsed = ((now - start).nanos).toMillis
    println(s"$message: TimeElapsedInMS: $timeElapsed")
  }

  def printType(tpe: __Type): String = {
    tpe.ofType match {
      case Some(inner) => s"""${tpe.name.getOrElse("THISMAKESLITTLESENSE")} of ${printType(inner)}"""
      case None => tpe.name.getOrElse("THISMAKESNOSENSE")
    }
  }

  def genericEntityHandler[R](extensions: ExtensionLogicMap, tables: List[Table], conditionsHandlers: Map[String,ObjectValue => String], connection: Connection)(value: InputValue): ZQuery[R, CalibanError, Step[R]] = {
    ZQuery.succeed(Step.MetadataFunctionStep(field => QueryStep(ZQuery.fromEffect(ZIO{
      val start = System.nanoTime()
      // Convert to intermediate Representation
      val intermediate = toIntermediateV2(field, tables, "_entities", extensions.map(_._1), rootLevelOfEntityResolver = true)
      // Extract the keys from the arguments
      val condition = value match {
        case o @ ObjectValue(fields) =>
          val tpeName = InputValue.circeEncoder.apply(fields("__typename")).as[String].getOrElse(throw new RuntimeException("Should never happen"))
          conditionsHandlers(tpeName)(o)
        case _ => throw new RuntimeException("should never happen")
      }
      // Convert to query
      val query = intermdiateV2ToSQL(intermediate, "root", Option(condition))
      printTime(start, "Query Compilation")
      println(query)


      // Prepare the statement
      // Execute the query
      // Extract the result
      // Parse the json
      // Apply all the modifications from our extensions
      // Parse to ResponseValue and return

      // Caliban federation is weird
      val coordinatesForTypenameQueries = checkForTypenameQuery(field, Nil).map { case (path, tpe) =>
        println(s"PATH: $path")
        path match {
          case Nil => path -> tpe
          case ::(_, tl) => tl -> tpe
        }
      }
      val cooridnatesDebug = coordinatesForTypenameQueries.map { case (path, typename) => s"${path} -> $typename"}.mkString("\n")

      val stuff =
        s"""|DEBUGGING
           |${printQuery(field)}
           |$cooridnatesDebug""".stripMargin
      println(stuff)
      val typeNameInsertions = constructTypenameInserters(coordinatesForTypenameQueries)

      for {
        preparedStatement <- ZIO(connection.prepareStatement(query))
        _ = printTime(start, "Prepared statemetn")
        resultSet <-ZIO(preparedStatement.executeQuery())
        _ = printTime(start, "Quuery execution")
        resultString <- ZIO{
          resultSet.next()
          resultSet.getString("root")
        }
        resultAsJson <- ZIO.fromEither(io.circe.parser.parse(resultString))
        _ = printTime(start, "Result parting")
        resultWithExtensions <- applyExtensions2(field,extensions.map{case (k,v) => k -> v.apply(field)}, resultAsJson, true)
        _ = printTime(start, "EXtensions")
        resultWithTypeNames = typeNameInsertions.foldLeft(resultWithExtensions)((acc, next) => next(acc))
        // Entity queries always return non null list of non null
        tpeBefore = printType(field.fieldType)
        whatever = field.copy(fieldType = field.fieldType.ofType.flatMap(_.ofType).getOrElse(throw new RuntimeException("Entity resolver query does not have inner type of inner type: Should never happen")))
        tpeAfter = printType(whatever.fieldType)
        resultStripped = stripUnwantedStuff(resultWithTypeNames, whatever)
        _ = printTime(start, "Stripping")
        resultAsResponseValue <- ZIO.fromEither(ResponseValue.circeDecoder.decodeJson(resultStripped))
        debugPrint =
          s"""
             |----------------------------------------
             |"Database query"
             |$query
             |QueryDebug
             |$stuff
             |DatabaseResult
             |$resultAsJson
             |DatabaseResult with extensions
             |$resultWithExtensions
             |DatabaseResult with extensions and typename
             |$resultWithTypeNames
             |ResultStripped
             |$resultStripped
             |TPEBEFORE
             |$tpeBefore
             |TPEAFTER
             |$tpeAfter
             |--------------------------------------
             |""".stripMargin
        _ <- ZIO(println(debugPrint))
      } yield PureStep(resultAsResponseValue)

    }.flatten.tapError(e => ZIO(e.printStackTrace()))))))
  }


  def constructQueryOperation[R, S](connection: Connection, extensions: ExtensionLogicMap, tables: List[Table])(implicit schema: Schema[R,S]): Operation[R] = {
    val tpe = schema.toType_()

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
                                        extensions: ExtensionLogicMap,
                                        tables: List[Table],
                                        mutationToInsertReader: Map[String, Json => String],
                                        mutationToUpdateReader: Map[String, Json => String],
                                        mutationToDeleteReader: Map[String, Json => String])(implicit schema: Schema[R,S]): Operation[R] = {
    val tpe = schema.toType_()

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