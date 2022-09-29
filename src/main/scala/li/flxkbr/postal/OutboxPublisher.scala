package li.flxkbr.postal

import scala.concurrent.duration.DurationInt

import li.flxkbr.postal.util.KillSwitch
import li.flxkbr.postal.config.*
import li.flxkbr.postal.db.dao.OutboxRecordDAO
import li.flxkbr.postal.db.{OutboxRecord, RecordId}
import li.flxkbr.postal.log.{DefaultIOLogging, loggedShow, loggedRaw}
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
    kafkaProducer: KafkaProducer[IO, Option[String], Array[Byte]],
)(using
    pCfg: OutboxPublisherConfig,
    codec: OutboxRecord => ProducerRecord[Option[String], Array[Byte]],
) extends DefaultIOLogging {

  val WriteChunkSize = 20
  val WriteMaxDelay  = 500.millis

  import org.legogroup.woof.given_LogInfo

  def run = {
    for {
      switch <- Deferred[IO, Unit]
      handle <- (Stream
        .fixedRateStartImmediately[IO](
          pCfg.rate,
          dampen = true,
        ) >> outboxRecordDao.unpublishedStream.take(pCfg.maxMessages))
        .loggedShow(LogLevel.Info)
        .through(publish)
        .loggedRaw()
        .through(markPublished)
        .interruptWhen(switch.get.attempt)
        .compile
        .drain
        .start
    } yield KillSwitch("outbox-publisher", switch, handle)
  }

  protected val publish
      : Pipe[IO, OutboxRecord, ProducerResult[OutboxRecord, Option[
        String,
      ], Array[Byte]]] =
    _.map { rec => ProducerRecords.one(codec(rec), rec) }
      .parEvalMapUnordered(pCfg.publisherParallelism) { records =>
        kafkaProducer
          .produce(records)
          .flatten
          .onError { t =>
            logger.map(
              _.error(s"Failed to publish records $records: $t"),
            )
          }
          .attempt
      }
      .collect { case Right(result) =>
        result
      }

  protected val markPublished: Pipe[IO, ProducerResult[OutboxRecord, Option[
    String,
  ], Array[Byte]], Unit] =
    _.groupWithin(WriteChunkSize, WriteMaxDelay).evalMap {
      _.toNel match
        case Some(results) =>
          for {
            count <- outboxRecordDao.setPublished(results.map(_.passthrough.id))
            log   <- logger
            _     <- log.trace(s"Committed $count messages as published")
          } yield ()
        case None => logger.flatMap(_.warn("Received empty chunk"))
    }
}
