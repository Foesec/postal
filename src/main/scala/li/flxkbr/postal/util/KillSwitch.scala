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
    completed <- switch.complete(())
    outcome   <- handleFb.join
  } yield outcome

  def kill(timeout: FiniteDuration): IO[Outcome[IO, Throwable, Unit]] =
    term().timeoutTo(
      timeout,
      for {
        log <- logger
        _ <- log.error(
          s"Failed to terminate killswitch $name with fiber ${handleFb.toString()}",
        )
        _ <- handleFb.cancel.timeoutTo(timeout, IO.unit).start
      } yield Outcome.errored(
        TimeoutException(s"Failed to terminate killswitch $name"),
      ),
    )
}
