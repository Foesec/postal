package li.flxkbr.postal.log

import cats.Show
import cats.syntax.flatMap.toFlatMapOps
import cats.FlatMap
import cats.effect.IO
import cats.effect.std.Console
import fs2.Stream
import org.legogroup.woof.ColorPrinter.Theme
import org.legogroup.woof.*

trait DefaultIOLogging extends IOLoggingExtensions {
  private given Filter  = Filter.everything
  private given Printer = ColorPrinter()

  given logger(using Console[IO]): IO[Logger[IO]] =
    DefaultLogger.makeIo(Output.fromConsole)
}

trait IOLoggingExtensions {
  extension [F[_]: FlatMap](logger: F[Logger[F]])
    def doLog(level: LogLevel, msg: String)(using LogInfo): F[Unit] =
      logger.flatMap(_.doLog(level, msg))
    def info(msg: String)(using LogInfo): F[Unit] = logger.flatMap(_.info(msg))
    def warn(msg: String)(using LogInfo): F[Unit] = logger.flatMap(_.warn(msg))
    def error(msg: String)(using LogInfo): F[Unit] =
      logger.flatMap(_.error(msg))
    def trace(msg: String)(using LogInfo): F[Unit] =
      logger.flatMap(_.trace(msg))
}

extension [O: Show](s: Stream[IO, O])
  def loggedShow(
      level: LogLevel = LogLevel.Info,
  )(using log: IO[Logger[IO]], li: LogInfo): Stream[IO, O] = s.evalTap { el =>
    log.flatMap(_.doLog(level, Show[O].show(el)))
  }

extension [O](s: Stream[IO, O])
  def loggedRaw(
      level: LogLevel = LogLevel.Info,
  )(using log: IO[Logger[IO]], li: LogInfo): Stream[IO, O] = s.evalTap { el =>
    log.flatMap(_.doLog(level, el.toString))
  }
