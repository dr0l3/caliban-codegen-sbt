package runner

import caliban.{GraphQL, Http4sAdapter}
import caliban.execution.Field
import cats.data.Kleisli
import org.http4s.StaticFile
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.Router
import org.http4s.server.middleware.CORS
import zio.{App, ExitCode, RIO, URIO, ZEnv, ZIO}

import scala.concurrent.ExecutionContext

object DefaultRunner {
  def runApi(api: GraphQL[Any], port: Int): URIO[zio.ZEnv, ExitCode] = {
    import zio.interop.catz._

    type Whatever[A] = RIO[ZEnv, A]

    (for {
      interpreter <- api.interpreter
      _ = println(api.render)
      exit <- BlazeServerBuilder[Whatever](ExecutionContext.global).bindHttp(port, "localhost")
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
