//package codegen2
//
//import caliban.{CalibanError, Http4sAdapter}
//import caliban.execution.Field
//import caliban.federation.tracing.ApolloFederatedTracing
//import cats.data.Kleisli
//import org.http4s.StaticFile
//import org.http4s.blaze.server.BlazeServerBuilder
//import org.http4s.implicits.http4sKleisliResponseSyntaxOptionT
//import org.http4s.server.Router
//import org.http4s.server.middleware.CORS
//import zio._
//
//import java.sql.DriverManager
//import scala.concurrent.ExecutionContext
//
//object SuperApp extends App {
//  import codegen.PostgresSniffer.connToTables
//  val _ = classOf[org.postgresql.Driver]
//  val con_str = "jdbc:postgresql://0.0.0.0:5438/product"
//  implicit val conn = DriverManager.getConnection(con_str, "postgres", "postgres")
//
//  val extensionsImpl = new Extensions {
//    override def usersaddress(field: Field, args: usersaddressExtensionArgs): ZIO[Any, Throwable, String] = ZIO(s"WOOO: ${args.id}")
//  }
//
//  val api = new API(extensionsImpl, conn).createApi()
//
//
//  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
//    import zio.interop.catz._
//
//    type Whatever[A] = RIO[ZEnv, A]
//
//    (for {
//      interpreter <- api.@@(ApolloFederatedTracing.wrapper).interpreter
//      exit <- BlazeServerBuilder[Whatever](ExecutionContext.global).bindHttp(8088, "localhost")
//        .withHttpApp(
//          Router[Whatever](
//            "/graphql" -> CORS(Http4sAdapter.makeHttpService(interpreter)),
//            "/graphiql"    -> Kleisli.liftF(StaticFile.fromResource("/graphiql.html", None))
//          ).orNotFound
//        )
//        .resource
//        .toManagedZIO
//        .useForever
//        .exitCode
//    } yield exit).exitCode
//  }
//}
