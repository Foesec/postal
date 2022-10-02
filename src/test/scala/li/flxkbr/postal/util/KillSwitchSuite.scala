package li.flxkbr.postal.util

import scala.concurrent.duration.{Duration, DurationInt}

import cats.effect.IO
import cats.effect.kernel.{Deferred, Outcome}
import java.util.concurrent.TimeoutException
import li.flxkbr.postal.testkit.ExtendedAssertions
import cats.effect.kernel.Ref
import cats.effect.kernel.Outcome.Succeeded
import cats.effect.kernel.Outcome.Errored
import cats.effect.kernel.Outcome.Canceled
import li.flxkbr.postal.log.DefaultIOLogging

class KillSwitchSuite
    extends munit.CatsEffectSuite
    with ExtendedAssertions
    with DefaultIOLogging {

  import org.legogroup.woof.given_LogInfo

  test("killswitch term() terminates associated task") {
    for {
      switch <- Deferred[IO, Unit]
      handle <- switch.get.start
      ks = KillSwitch("ks-term-test-1", switch, handle)
      outcome <- ks.term().timeoutAndForget(2.seconds)
    } yield assertEquals(outcome, Outcome.succeeded(IO.unit))
  }

  test("killswitch kill() terminates associated task within timeout") {
    for {
      switch <- Deferred[IO, Unit]
      handle <- (IO.sleep(500.millis) >> switch.get).start
      ks = KillSwitch("ks-kill-test-1", switch, handle)
      outcome <- ks.kill(1.second, false).timeoutAndForget(2.seconds)
    } yield assertEquals(outcome, Outcome.succeeded(IO.unit))
  }

  test("killswitch kill() cancels associated tasks after timeout") {
    for {
      switch       <- Deferred[IO, Unit] // dummy, doesnt do anything
      verifyCancel <- Ref[IO].of(false)  // ref to check if cancelled
      handle <- (IO
        .never[Unit])
        .onCancel(verifyCancel.set(true))
        .start
      ks = KillSwitch("ks-kill-test-2", switch, handle)
      outcome <- ks
        .kill(250.millis, awaitCancel = true)
        .timeoutAndForget(2.seconds)
      _ <- assertIO(verifyCancel.get, true)
    } yield {
      outcome match
        case Errored(t) => assert(t.isInstanceOf[TimeoutException])
        case x          => fail(s"Outcome was not Errored but $x")
    }
  }

  test("killswitch kill() does not await cancel if awaitCancel = false") {
    for {
      switch       <- Deferred[IO, Unit] // dummy, doesnt do anything
      verifyCancel <- Ref[IO].of(false)  // ref to check if cancelled
      handle <- (IO
        .uncancelable(_ => IO.sleep(500.millis)) *> IO.never[Unit])
        .onCancel(verifyCancel.set(true))
        .start
      ks = KillSwitch("ks-kill-test-2", switch, handle)
      outcome <- ks
        .kill(250.millis, awaitCancel = false)
        .timeoutAndForget(2.seconds)
      _ <- assertIO(verifyCancel.get, false)
      _ <- IO.sleep(300.millis)
      _ <- assertIO(verifyCancel.get, true)
    } yield {
      outcome match
        case Errored(t) => assert(t.isInstanceOf[TimeoutException])
        case x          => fail(s"Outcome was not Errored but $x")
    }
  }
}
