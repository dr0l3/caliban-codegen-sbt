package runner

import java.sql.{Connection, DriverManager}
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object Whatever extends App {
  Class.forName("org.postgresql.Driver")
  val _ = classOf[org.postgresql.Driver]
  val con_str =
    "jdbc:postgresql://0.0.0.0:31337/test2" //TODO: How do we do this??

  val conn = DriverManager.getConnection(con_str, "postgres", "postgres")

  val statements = List(
    "select json_build_object('id', dataz.id, 'name', dataz.name, 'email', dataz.email) from (select * from users limit 1) as dataz;",
    "select row_to_json(dataz) from (select * from users limit 1) as dataz;",
    "select * from users limit 1;",
  )

  statements.foreach { statement =>
    val wahtever = conn.prepareStatement(statement)
    println("Warming up")
    (0 to 100).foreach(_ => wahtever.executeQuery())
    println(s"Starting $statement")
    val start = System.nanoTime()
    (0 to 10000).foreach(_ => wahtever.executeQuery())
    val end = System.nanoTime()
    println(s"$statement -> ${(end-start).nanos.toMillis}")
    Thread.sleep(5000)
  }
}

object Whatever3 extends App {
  Class.forName("org.postgresql.Driver")
  val _ = classOf[org.postgresql.Driver]
  val con_str =
    "jdbc:postgresql://0.0.0.0:31337/test2" //TODO: How do we do this??

  val conn = DriverManager.getConnection(con_str, "postgres", "postgres")

  val statements = List(
    "select * from (select * from users) u left join orders o on u.id = o.user_id;",
    "SELECT\n    coalesce(json_agg(\"root\"), '[]') AS \"root\"\nFROM\n    (\n        SELECT\n            row_to_json(\n                    (\n                        SELECT\n                            \"_5_e\"\n                        FROM\n                            (\n                                SELECT\n                                    \"_0_root.base\".\"id\" AS \"id\",\n                                    \"_0_root.base\".\"email\" AS \"email\",\n                                    \"_4_root.ar.root.orders\".\"orders\" AS \"orders\"\n                            ) AS \"_5_e\"\n                    )\n                ) AS \"root\"\n        FROM\n            (\n                SELECT\n                    *\n                FROM\n                    \"public\".\"users\"\n                WHERE\n                    ('true')\n            ) AS \"_0_root.base\"\n                LEFT OUTER JOIN LATERAL (\n                SELECT\n                    coalesce(json_agg(\"orders\"), '[]') AS \"orders\"\n                FROM\n                    (\n                        SELECT\n                            row_to_json(\n                                    (\n                                        SELECT\n                                            \"_2_e\"\n                                        FROM\n                                            (\n                                                SELECT\n                                                    \"_1_root.ar.root.orders.base\".\"id\" AS \"id\",\n                                                    \"_1_root.ar.root.orders.base\".\"time\" AS \"time\"\n                                            ) AS \"_2_e\"\n                                    )\n                                ) AS \"orders\"\n                        FROM\n                            (\n                                SELECT\n                                    *\n                                FROM\n                                    \"public\".\"orders\"\n                                WHERE\n                                    ((\"_0_root.base\".\"id\") = (\"user_id\"))\n                            ) AS \"_1_root.ar.root.orders.base\"\n                    ) AS \"_3_root.ar.root.orders\"\n                ) AS \"_4_root.ar.root.orders\" ON ('true')\n    ) AS \"_6_root\"",
  )

  statements.foreach { statement =>
    val wahtever = conn.prepareStatement(statement)
    println("Warming up")
    (0 to 100).foreach(_ => wahtever.executeQuery())
    println(s"Starting $statement")
    val start = System.nanoTime()
    (0 to 1000).foreach{i =>
      if (i% 250 == 0) println(s"${i}")
      wahtever.executeQuery()
    }
    val end = System.nanoTime()
    println(s"$statement -> ${(end-start).nanos.toMillis}")
    Thread.sleep(5000)
  }
}

object Whatever4 extends App {
  Class.forName("org.postgresql.Driver")
  val _ = classOf[org.postgresql.Driver]
  val con_str =
    "jdbc:postgresql://0.0.0.0:31337/test2" //TODO: How do we do this??

  val conn = DriverManager.getConnection(con_str, "postgres", "postgres")

  val statements = List(
    "select * from (select * from users) u left join orders o on u.id = o.user_id;",
    "SELECT\n    coalesce(json_agg(\"root\"), '[]') AS \"root\"\nFROM\n    (\n        SELECT\n            row_to_json(\n                    (\n                        SELECT\n                            \"_5_e\"\n                        FROM\n                            (\n                                SELECT\n                                    \"_0_root.base\".\"id\" AS \"id\",\n                                    \"_0_root.base\".\"email\" AS \"email\",\n                                    \"_4_root.ar.root.orders\".\"orders\" AS \"orders\"\n                            ) AS \"_5_e\"\n                    )\n                ) AS \"root\"\n        FROM\n            (\n                SELECT\n                    *\n                FROM\n                    \"public\".\"users\"\n                WHERE\n                    ('true')\n            ) AS \"_0_root.base\"\n                LEFT OUTER JOIN LATERAL (\n                SELECT\n                    coalesce(json_agg(\"orders\"), '[]') AS \"orders\"\n                FROM\n                    (\n                        SELECT\n                            row_to_json(\n                                    (\n                                        SELECT\n                                            \"_2_e\"\n                                        FROM\n                                            (\n                                                SELECT\n                                                    \"_1_root.ar.root.orders.base\".\"id\" AS \"id\",\n                                                    \"_1_root.ar.root.orders.base\".\"time\" AS \"time\"\n                                            ) AS \"_2_e\"\n                                    )\n                                ) AS \"orders\"\n                        FROM\n                            (\n                                SELECT\n                                    *\n                                FROM\n                                    \"public\".\"orders\"\n                                WHERE\n                                    ((\"_0_root.base\".\"id\") = (\"user_id\"))\n                            ) AS \"_1_root.ar.root.orders.base\"\n                    ) AS \"_3_root.ar.root.orders\"\n                ) AS \"_4_root.ar.root.orders\" ON ('true')\n    ) AS \"_6_root\"",
  )

  def firstCase() = {
    val statemetn = conn.prepareStatement("SELECT\n    coalesce(json_agg(\"root\"), '[]') AS \"root\"\nFROM\n    (\n        SELECT\n            row_to_json(\n                    (\n                        SELECT\n                            \"_5_e\"\n                        FROM\n                            (\n                                SELECT\n                                    \"_0_root.base\".\"id\" AS \"id\",\n                                    \"_0_root.base\".\"email\" AS \"email\",\n                                    \"_4_root.ar.root.orders\".\"orders\" AS \"orders\"\n                            ) AS \"_5_e\"\n                    )\n                ) AS \"root\"\n        FROM\n            (\n                SELECT\n                    *\n                FROM\n                    \"public\".\"users\"\n                WHERE\n                    ('true')\n            ) AS \"_0_root.base\"\n                LEFT OUTER JOIN LATERAL (\n                SELECT\n                    coalesce(json_agg(\"orders\"), '[]') AS \"orders\"\n                FROM\n                    (\n                        SELECT\n                            row_to_json(\n                                    (\n                                        SELECT\n                                            \"_2_e\"\n                                        FROM\n                                            (\n                                                SELECT\n                                                    \"_1_root.ar.root.orders.base\".\"id\" AS \"id\",\n                                                    \"_1_root.ar.root.orders.base\".\"time\" AS \"time\"\n                                            ) AS \"_2_e\"\n                                    )\n                                ) AS \"orders\"\n                        FROM\n                            (\n                                SELECT\n                                    *\n                                FROM\n                                    \"public\".\"orders\"\n                                WHERE\n                                    ((\"_0_root.base\".\"id\") = (\"user_id\"))\n                            ) AS \"_1_root.ar.root.orders.base\"\n                    ) AS \"_3_root.ar.root.orders\"\n                ) AS \"_4_root.ar.root.orders\" ON ('true')\n    ) AS \"_6_root\"")

    (0 to 100).foreach(_ => statemetn.executeQuery())
    val start = System.nanoTime()
    (0 to 1000).foreach { i =>
      if(i % 250 == 0) println(s"$i")
      val result = statemetn.executeQuery()
      result.next()
      val json = result.getString(1)
      val parsed = io.circe.parser.parse(json)

    }
    val end = System.nanoTime()
    println(s"Second case: ${(end-start).nanos.toMillis}")
  }



  def secondCase(): Unit = {
    import io.circe.syntax._
    import io.circe._, io.circe.generic.semiauto._

    case class User(id: UUID, name: String, email: String, orders: List[Order])
    case class Order(id: UUID, time: String)

    implicit val orderDecoder = deriveEncoder[Order]
    implicit val userDecoder = deriveEncoder[User]

    val statement = conn.prepareStatement("select *, o.id as order_id from (select * from users) u left join orders o on u.id = o.user_id;")
    (0 to 100).foreach(_ => statement.executeQuery())
    val start = System.nanoTime()
    (0 to 1000).foreach { i =>
      if(i % 250 == 0) println(s"$i")
      val result = statement.executeQuery()
      val listBuffer = ListBuffer.newBuilder[User]
      while(result.next()) {
        val userId = UUID.fromString(result.getString("user_id"))
        val name = result.getString("name")
        val email = result.getString("email")
        val orderId = UUID.fromString(result.getString("order_id"))
        val time = result.getString("time")
        listBuffer.+=(User(userId, name, email, List(Order(orderId,time))))
      }
      val inMemory = listBuffer.result().toList.groupBy(_.id).map { case (_, users) =>
        users.head.copy(orders = users.flatMap(_.orders))
      }
      val json = inMemory.asJson
    }
    val end = System.nanoTime()

    println(s"Second case: ${(end-start).nanos.toMillis}")
  }

  firstCase()
  secondCase()
}

object Whatever2 extends App {
  Class.forName("org.postgresql.Driver")
  val _ = classOf[org.postgresql.Driver]
  val con_str =
    "jdbc:postgresql://0.0.0.0:31337/test2" //TODO: How do we do this??

  val conn = DriverManager.getConnection(con_str, "postgres", "postgres")

  val statement = conn.prepareStatement("select * from users")
  val insert = conn.prepareStatement("insert into orders(user_id) values (?)")
  val results = statement.executeQuery()

  while(results.next()) {
    val id = results.getString("id")

    (0 to 10).foreach { _ =>
      val stmt = conn.createStatement()
      stmt.executeUpdate(s"insert into orders(user_id) values ('$id')")
    }
  }
}


import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import com.impossibl.postgres.jdbc.PGDataSource;
import com.impossibl.postgres.jdbc.PGDriver


class Listen extends PGNotificationListener {
  override def notification(processId: Int, channelName: String, payload: String): Unit = {
    println(s"$processId -> $channelName: $payload")
  }
}

object DdlSubsription extends App {
  Class.forName("org.postgresql.Driver")
  val _ = classOf[com.impossibl.postgres.jdbc.PGDriver]

  val con_str =
    "jdbc:pgsql://0.0.0.0:5439/notifications" //TODO: How do we do this??
  val dataSource = new PGDataSource()
  dataSource.setDatabaseUrl(con_str)
  dataSource.setUser("postgres")
  dataSource.setPassword("postgres")


  val conn = dataSource.getConnection.unwrap(classOf[PGConnection])



  conn.addNotificationListener(new Listen())


  val statemen = conn.createStatement()

  statemen.executeUpdate("LISTEN postgraphile_watch")
  statemen.close()

  Thread.sleep(10000000)
}