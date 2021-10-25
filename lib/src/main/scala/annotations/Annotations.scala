package annotations

import codegen.Table
import zio._
import zio.prelude.Validation

import scala.language.postfixOps

sealed trait TableAnnotation
// @Omit
case object OmitTable extends TableAnnotation
// @Union(User, user_type, {admin: Admin, agent: Agent, enduser: Enduser}, {Agent: [id, email, title], Admin: [id, title], Enduser: [id, nickname]}, {Agent: [id], Admin:[id], Enduser: [id, nickname]})
case class UnionTable(unionName: String, discriminator: String, typeByDiscriminatorValue: Map[String, String], fieldsByType: Map[String, List[String]], requiredFieldsByType: Map[String, List[String]]) extends TableAnnotation

object UnionTable {
  def validate(table: Table, unionAnnotation: UnionTable) = {
    for {
      _ <- Validation.fromPredicateWith(s"All referenced columns should be present")(unionAnnotation) { union =>
        val allReferencedColumns = List(union.discriminator) ++ union.fieldsByType.values.flatten
        (allReferencedColumns intersect (table.otherColumns ++ table.primaryKeys).map(_.name)) == allReferencedColumns
      }
      _ <- Validation.fromPredicateWith(s"All types should have fields")(unionAnnotation){ union =>
        union.fieldsByType.forall {case (_, v) => v.nonEmpty}
      }
      _ <- Validation.fromPredicateWith("All primary keys should be present in all childtypes")(unionAnnotation){ union =>
        val primaryKeys = table.primaryKeys.map(_.name)
        union.fieldsByType.forall { case (_, v) => primaryKeys.intersect(v) == primaryKeys}
      }
      _ <- Validation.fromPredicateWith("All primary keys should be required in all childtypes")(unionAnnotation){ union =>
        val primaryKeys = table.primaryKeys.map(_.name)
        union.requiredFieldsByType.forall { case (_, v) => primaryKeys.intersect(v) == primaryKeys}
      }
      _ <- Validation.fromPredicateWith("All mentioned types should be distinct")(unionAnnotation){ union =>
        val allTypes = List(union.unionName) ++ union.typeByDiscriminatorValue.values
        allTypes == allTypes.distinct
      }
      _ <- Validation.fromPredicateWith("All required columns should be present in columns")(unionAnnotation) { union =>
        union.requiredFieldsByType.toList.intersect(union.fieldsByType.toList) == union.requiredFieldsByType.toList
      }
      // TODO: Discriminator should be either enum, string, number or enumtable
    } yield unionAnnotation
  }
}

case object Whatver

import fastparse._
import SingleLineWhitespace._


object TableAnnotationParser {
  def omit[_:P] : P[TableAnnotation] = P("@omit").map(_ => OmitTable)
  def name[_: P]: P[String]                               = P(CharIn("_A-Za-z").rep).!

  def stringList[_:P]: P[List[String]] =  P("[" ~/ name.rep(sep= ",") ~ "]").map(_.toList)
  def stringStringMapEntry[_:P]: P[(String, String)] = P(name~ ":" ~ name)
  def stringStringMap[_:P] : P[Map[String, String]] = P("{" ~ stringStringMapEntry.rep(sep= ",") ~ "}").map(_.toMap)

  def stringListMapEntry[_:P]: P[(String, List[String])] = P(name ~ ": " ~ stringList)
  def stringListMap[_:P] : P[Map[String, List[String]]] = P("{" ~ stringListMapEntry.rep(sep = ",") ~ "}").map(_.toMap)
  def unionTable[_:P]: P[UnionTable] = P("@union(" ~ name ~ "," ~ name ~ "," ~ stringStringMap ~ "," ~ stringListMap ~ "," ~ stringListMap ~ ")").map((UnionTable.apply _).tupled)
  def tableAnnotation[_:P]: P[TableAnnotation] = P(omit | unionTable)
}

object Test extends App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = ZIO{
    val annotaiton = "@union(User, user_type, {admin: Admin, agent: Agent, enduser: Enduser}, {Agent: [id, email, title], Admin: [id, title], Enduser: [id, nickname]}, {Agent: [id, email], Admin: [id], Enduser: [id]})"
    //  val annotaiton = "@omit"

    val result = fastparse.parse(annotaiton, TableAnnotationParser.unionTable(_), true)
    result match {
      case Parsed.Success(value, index) => println(value)
      case failure: Parsed.Failure => println(failure)
    }

    val wahtever = "{ admin : Admin, agent : Agent, enduser : Enduser }"

    println(fastparse.parse(wahtever, TableAnnotationParser.stringStringMap(_)))
  }.exitCode
}