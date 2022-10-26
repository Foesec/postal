package li.flxkbr.postal

import cats.data.NonEmptyList
import cats.effect.*
import cats.effect.std.Console
import cats.implicits.{catsSyntaxEitherId, toTraverseOps}
import cats.syntax.all.catsSyntaxApplicativeError
import doobie.*
import doobie.implicits.*
import fs2.*
import fs2.kafka.*
import li.flxkbr.postal.config.*
import li.flxkbr.postal.db.dao.OutboxRecordDAO
import li.flxkbr.postal.db.{OutboxRecord, RecordId}
import li.flxkbr.postal.log.{DefaultIOLogging, loggedRaw, loggedShow}
import li.flxkbr.postal.util.{KillSwitch, StreamExtensions}
import org.legogroup.woof.LogLevel

import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

class OutboxPublisher(
    outboxRecordDao: OutboxRecordDAO,
    kafkaProducer: OutboxKafkaProducer,
)(using
    pCfg: OutboxPublisherConfig,
) extends DefaultIOLogging
    with StreamExtensions {

  import org.legogroup.woof.given_LogInfo

  def run: IO[KillSwitch[Unit]] = {
    for {
      sig <- Deferred[IO, Unit]
      handle <- buildRestartingStream
        .interruptWhen(sig.get.attempt)
        .compile
        .drain
        .start
    } yield KillSwitch("outbox-publisher", sig, handle)
  }

  protected def buildRestartingStream: Stream[IO, Unit] = {
    (Stream
      .fixedRateStartImmediately[IO](
        pCfg.rate,
        dampen = true,
      ) >> outboxRecordDao.unpublishedStream.take(pCfg.maxMessages))
      .loggedShow(LogLevel.Trace)
      .through(uncancelablePublishAndWriteback)
      .recoverWith { case NonFatal(t) =>
        Stream.exec(
          logger.warn(s"Stream failed due to $t. Restarting..."),
        ) ++ buildRestartingStream
      }
  }

  protected val uncancelablePublishAndWriteback: Pipe[IO, OutboxRecord, Unit] =
    _.groupWithin(pCfg.publishBatchSize, pCfg.publishMaxDelay)
      .parEvalMap(
        pCfg.publisherParallelism,
      ) { records =>
        records.toNel match
          case Some(recordsNel) =>
            val prodRecs =
              ProducerRecords.chunk(
                records.map(_.toProducerRecord),
                recordsNel.map(_.id),
              )
            IO.uncancelable { _ =>
              for {
                producerResult <- kafkaProducer
                  .produce(prodRecs)
                  .flatten
                  .onError { t =>
                    logger.error(s"Failed to publish records $records: $t")
                  }
                _ <- logger.trace(
                  s"Outbox published ${producerResult.records.size} records",
                )
                ids = producerResult.passthrough
                count <- outboxRecordDao.setPublished(ids).onError { t =>
                  logger.error(s"Failed to write back published records: $t")
                }
                _ <- logger.trace(
                  s"Outbox committed $count records as published",
                )
              } yield ()
            }.attempt
          case None => logger.warn("Received empty chunk to commit").attempt
      }
      .evalTapChunk { (result: Either[Throwable, Unit]) =>
        result match
          case Left(t)  => logger.warn(s"Failure during publish+writeback: $t")
          case Right(_) => IO.unit
      }
      .collectRight
}

type OutboxKafkaProducer = KafkaProducer[IO, Option[Array[Byte]], Array[Byte]]
