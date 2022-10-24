import fs2.*
import fs2.concurrent.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.DurationInt
import cats.implicits.*
import cats.syntax.all.*

val s = Stream(1, 2, 3).covary[IO].evalMap { n => IO.println(s"num: $n") }


val t = Stream.exec(IO.println("STARTING")) ++ s ++ Stream.eval(IO.println("ENDING"))

val res: IO[Unit] = t.compile.drain

res.unsafeRunSync()