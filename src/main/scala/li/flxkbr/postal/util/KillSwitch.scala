package li.flxkbr.postal.util

import cats.effect.{IO, Ref}
import cats.effect.kernel.Outcome
import cats.effect.kernel.Deferred
import cats.effect.kernel.Fiber

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt
import li.flxkbr.postal.log.DefaultIOLogging
import org.legogroup.woof.LogInfo

import java.util.concurrent.TimeoutException

trait KillSwitch[R] extends DefaultIOLogging {

  given logInfo: LogInfo

  val name: String
  protected val handleFb: Fiber[IO, Throwable, R]

  def term: IO[Outcome[IO, Throwable, R]]
  def kill(
      termTimeout: FiniteDuration,
      awaitCancel: Boolean = true,
  ): IO[Outcome[IO, Throwable, R]] =
    term.timeoutTo(
      termTimeout,
      for {
        log <- logger
        _ <- log.error(
          s"Failed to terminate killswitch $name",
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

object KillSwitch {
  def apply[R](
      name: String,
      sig: Deferred[IO, Unit],
      handleFb: Fiber[IO, Throwable, R],
  ): KillSwitch[R] = DeferredKillSwitch(name, sig, handleFb)

  def apply[R](
      name: String,
      sig: Ref[IO, Boolean],
      handleFb: Fiber[IO, Throwable, R],
  ): KillSwitch[R] = RefKillSwitch(name, sig, handleFb)
}

private[util] class RefKillSwitch[R](
    val name: String,
    sig: Ref[IO, Boolean],
    protected val handleFb: Fiber[IO, Throwable, R],
) extends KillSwitch[R]
    with DefaultIOLogging {

  override given logInfo: LogInfo = org.legogroup.woof.given_LogInfo

  def term: IO[
    Outcome[IO, Throwable, R],
  ] =
    for
      _       <- logger.info(s"Terminating killswitch $name")
      _       <- sig.set(true)
      outcome <- handleFb.join
    yield outcome
}

private[util] class DeferredKillSwitch[R](
    val name: String,
    switch: Deferred[IO, Unit],
    protected val handleFb: Fiber[IO, Throwable, R],
) extends KillSwitch[R]
    with DefaultIOLogging {

  override given logInfo: LogInfo = org.legogroup.woof.given_LogInfo

  def term: IO[Outcome[IO, Throwable, R]] = for {
    _       <- logger.info(s"Terminating killswitch $name")
    _       <- switch.complete(())
    outcome <- handleFb.join
  } yield outcome

}
