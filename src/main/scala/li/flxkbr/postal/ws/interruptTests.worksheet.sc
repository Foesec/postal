import cats.effect.unsafe.implicits.*
import cats.effect.*
import cats.effect.implicits.*
import cats.effect.std.Random
import cats.effect.IO.asyncForIO
import cats.implicits.*

import fs2.*
import org.legogroup.woof.{*, given}

import scala.concurrent.duration.DurationInt

def strim(
    s: Int,
    latch: Ref[IO, Boolean],
    shutdown: Deferred[IO, Unit],
): Stream[IO, Unit] =
  Stream(1, 2, 3)
    .covary[IO]
    .evalMap { n =>
      IO.println(s"eval $n") >>
        IO.uncancelable { poll =>
          IO.println(s"starting uncancelable $s:$n") >> latch.set(true) >> IO
            .sleep(
              50.millis,
            ) >> latch.set(false) >> IO.println(s"ending uncancelable $s:$n")
        }
    }
    .interruptWhen(shutdown.get.attempt)

val resIO: IO[List[Boolean]] = (1 to 50)
  .map { s =>
    for {
      latch      <- Ref.of[IO, Boolean](false)
      shutdown   <- Deferred[IO, Unit]
      str        <- strim(s, latch, shutdown).compile.drain.start
      rnd        <- Random.scalaUtilRandom[IO]
      dur        <- rnd.betweenInt(30, 100)
      _          <- IO.sleep(dur.millis)
      _          <- shutdown.complete(())
      _          <- str.join
      latchState <- latch.get
    } yield latchState
  }
  .toList
  .sequence

val res = resIO.unsafeRunSync()

res.forall(_ == false)
