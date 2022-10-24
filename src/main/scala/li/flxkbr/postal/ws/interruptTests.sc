import cats.effect.unsafe.implicits.*
import cats.effect.*
import cats.effect.implicits.*
import cats.effect.std.Random
import cats.effect.IO.asyncForIO

import fs2.*
import org.legogroup.woof.{*, given}

import scala.concurrent.duration.DurationInt

def mystream(
    dfr: Deferred[IO, Either[Throwable, Unit]],
    counter: Ref[IO, Int],
)(using log: Logger[IO]): Stream[IO, Unit] =
  Stream
    .fixedRateStartImmediately[IO](20.millis)
    .evalMap { _ =>
      counter.update(_ + 1) >> log.trace("STEP 1")
    }
    .evalMap { _ =>
      counter.update(_ + 1) >> log.trace("STEP 2")
    }
    .interruptWhen(dfr)

given Filter  = Filter.everything
given Printer = ColorPrinter()

val logger = DefaultLogger.makeIo(Output.fromConsole)

val random = Random.scalaUtilRandom[IO]

val ios: Seq[IO[(Int, Double)]] = (1 until 50).map { _ =>
  for {
    given Logger[IO] <- logger
    given Random[IO] <- random
    sig              <- Deferred[IO, Either[Throwable, Unit]]
    counter          <- Ref.of[IO, Int](0)
    str              <- mystream(sig, counter).compile.drain.start
    wait             <- Random[IO].nextIntBounded(500)
    _                <- IO.sleep(wait.millis)
    _                <- sig.complete(Right(()))
    _                <- str.join
    endCount         <- counter.get
    divided = endCount / 3.0f
    _ <- Logger[IO].error(s"Interrupted at $endCount/3 = $divided")
  } yield (endCount, divided)
}

ios.reduce(_ >> _).unsafeRunSync()
