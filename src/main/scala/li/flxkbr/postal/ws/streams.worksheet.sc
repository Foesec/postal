import fs2.*
import fs2.concurrent.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.DurationInt
import cats.implicits.*
import cats.syntax.all.*

Stream(1, 2, 3).covary[IO].meteredStartImmediately(500.millis).groupWithin(2, 1.second).evalMap(_.map(IO.println).sequence).compile.drain.unsafeRunSync()