package li.flxkbr.postal

import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

import li.flxkbr.postal.config.*
import li.flxkbr.postal.db.dao.OutboxRecordDAO
import li.flxkbr.postal.db.{OutboxRecord, RecordId}
import li.flxkbr.postal.log.{DefaultIOLogging, loggedRaw, loggedShow}
import li.flxkbr.postal.util.KillSwitch
import cats.data.NonEmptyList
import cats.effect.*
import cats.effect.std.Console
import cats.syntax.all.catsSyntaxApplicativeError
import doobie.*
import doobie.implicits.*
import fs2.*
import fs2.kafka.*
import org.legogroup.woof.LogLevel

class OutboxPublisher(
    outboxRecordDao: OutboxRecordDAO,
    kafkaProducer: OutboxKafkaProducer,
)(using
    pCfg: OutboxPublisherConfig,
    encoder: OutboxRecord => ProducerRecord[Option[Array[Byte]], Array[Byte]],
) extends DefaultIOLogging {

  import org.legogroup.woof.given_LogInfo

  def run = {
    for {
      switch <- Deferred[IO, Unit]
      handle <- {
        buildRestartingStream
          .interruptWhen(switch.get.attempt)
          .compile
          .drain
          .start
      }
    } yield KillSwitch("outbox-publisher", switch, handle)
  }

  protected def buildRestartingStream: Stream[IO, Unit] = {
    (Stream
      .fixedRateStartImmediately[IO](
        pCfg.rate,
        dampen = true,
      ) >> outboxRecordDao.unpublishedStream.take(pCfg.maxMessages))
      .loggedShow(LogLevel.Info)
      .through(publishAndWriteback)
      .recoverWith { case NonFatal(t) =>
        buildRestartingStream
      }

  }

  protected val publishAndWriteback: Pipe[IO, OutboxRecord, Unit] =
    _.map { rec => ProducerRecords.one(encoder(rec), rec) }
      .parEvalMapUnordered(pCfg.publisherParallelism) { records =>
        kafkaProducer
          .produce(records)
          .flatten
          .onError { t =>
            logger.error(s"Failed to publish records $records: $t")
          }
          .attempt
      }
      .collect { case Right(result) =>
        result
      }
      .groupWithin(pCfg.writebackChunkSize, pCfg.writebackMaxDelay)
      .evalMap {
        _.toNel match
          case Some(results) =>
            for {
              count <- outboxRecordDao.setPublished(
                results.map(_.passthrough.id),
              )
              _ <- logger.trace(s"Committed $count messages as published")
            } yield ()
          case None => logger.warn("Received empty chunk to commit")
      }
}

type OutboxKafkaProducer = KafkaProducer[IO, Option[Array[Byte]], Array[Byte]]
