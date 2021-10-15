import io.circe.optics.{JsonPath, JsonTraversalPath}
import io.circe.optics.JsonPath.root
import io.circe.{ACursor, Json, JsonObject}


sealed trait JsonPathPart
case object ArrayPath extends JsonPathPart
case class ObjectPath(fieldName: String) extends JsonPathPart

object Wtf extends App{
  import io.circe.optics.JsonOptics._
  val json = io.circe.parser.parse(""" { "hello": [{"this": "is dog"}, {"this": "rocks"}], "shit": {"fucking": "fest"} } """).right.get
  val parsedTwo = io.circe.parser.parse("""{ "hello": [{"this": {"message":"is dog"}}, {"this": {"message": "rocks"}}], "shit": {"fucking": "fest"} }""").right.get

  val jsonSelections: Seq[JsonPathPart] = List(ObjectPath("hello"), ArrayPath, ObjectPath("this"))

  def transformation(jsObject: Json): Json = {
    val merge = Json.obj("added" -> Json.fromString("wahtever"))
    jsObject.deepMerge(merge)
  }

  def transformObject(jsonObject: JsonObject): JsonObject = {
    println("TRANFORMATIN")
    jsonObject.add("added", Json.fromString("whatever"))
  }

//  val cursors =jsonSelections.foldLeft[List[ACursor]](List(json.hcursor)){(acc, next) =>
//    next match {
//      case codegen.PostgresSniffer.ObjectFieldSelection(fieldName) => acc.map(_.downField(fieldName))
//      case ArraySelection(fieldName) =>acc.flatMap(_.downField(fieldName).values.get.map(_.hcursor))
//    }
//  }

//  val wat = cursors.map(_.withFocus(transformation)).map(_.root.value)



//  println(json.hcursor.downField("shit").downField("fucking").withFocus(_.mapString(_.reverse)).top.get)
  val arary = json.hcursor.downField("hello")



//  println(arary.set(Json.fromValues(arary.values.get.map(_.hcursor.withFocus(transformation).top.get))).top.get)

  def applyTransformationAtPath(json: Json, transformation: Json => Json, path: List[JsonPathPart]) = {
    path match {
      case head :: rest =>
      case Nil => json.hcursor.withFocus(transformation).top.get
    }
  }


  sealed trait PathWrapper
  case class Object(path: JsonPath) extends PathWrapper
  case class Array(path: JsonTraversalPath) extends PathWrapper

  def createModification(transformer: JsonObject => JsonObject, path: List[JsonPathPart]): Json => Json = {
    path.foldLeft[PathWrapper](Object(root)) { (acc, next) =>
      next match {
        case ArrayPath => acc match {
          case Object(path) => Array(path.each)
          case Array(path) => Array(path.each)
        }
        case ObjectPath(fieldName) => acc match {
          case Object(path) => Object(path.selectDynamic(fieldName))
          case Array(path) => Array(path.selectDynamic(fieldName))
        }
      }
    } match {
      case Object(path) => path.obj.modify(transformer)
      case Array(path) =>path.obj.modify(transformer)
    }
  }

  val what = root.selectDynamic("hello").each.selectDynamic("this").obj.modify(transformObject)

  println(what(parsedTwo))

  val composedTransformer = createModification(transformObject, jsonSelections.toList)

  println(composedTransformer(parsedTwo))
}
