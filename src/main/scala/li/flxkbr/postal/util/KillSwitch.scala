package li.flxkbr.postal.util

import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.kernel.Deferred
import cats.effect.kernel.Fiber
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt
import li.flxkbr.postal.log.DefaultIOLogging
import java.util.concurrent.TimeoutException

class KillSwitch(
    val name: String,
    switch: Deferred[IO, Unit],
    handleFb: Fiber[IO, Throwable, Unit],
) extends DefaultIOLogging {

  import org.legogroup.woof.given_LogInfo

  def term(): IO[Outcome[IO, Throwable, Unit]] = for {
    _         <- logger.info(s"Terminating killswitch $name")
    completed <- switch.complete(())
    outcome   <- handleFb.join
  } yield outcome

  def kill(
      termTimeout: FiniteDuration,
      awaitCancel: Boolean = true,
  ): IO[Outcome[IO, Throwable, Unit]] =
    term().timeoutTo(
      termTimeout,
      for {
        log <- logger
        _ <- log.error(
          s"Failed to terminate killswitch $name with fiber ${handleFb.toString()}",
        )
        _ <-
          if awaitCancel then
            log.trace("Attempting to cancel associated task") >> handleFb.cancel
              .timeoutAndForget(5.seconds)
          else handleFb.cancel.timeoutAndForget(5.seconds).start
      } yield Outcome.errored(
        TimeoutException(s"Failed to terminate killswitch $name"),
      ),
    )
}
