package li.flxkbr.postal

import java.time.Instant
import scala.concurrent.duration.DurationInt

import li.flxkbr.postal.config.OutboxPublisherConfig
import li.flxkbr.postal.db.{OutboxRecord, RecordId}
import li.flxkbr.postal.testkit.*
import cats.effect.IO
import cats.effect.kernel.Outcome.*
import fs2.kafka.ProducerRecord

class OutboxPublisherSuite extends munit.CatsEffectSuite {

  test("outbox publisher correctly runs") {

    val createdTs = Instant.parse("2007-12-03T10:15:30.00Z")
    val unpublishedRecords = Seq(
      OutboxRecord(RecordId(1), "hello".getBytes(), createdTs, None),
      OutboxRecord(
        RecordId(2),
        "goodb".getBytes(),
        createdTs.plusSeconds(2),
        None,
      ),
      OutboxRecord(
        RecordId(3),
        "12345".getBytes(),
        createdTs.plusSeconds(4),
        None,
      ),
      OutboxRecord(
        RecordId(4),
        "xyzab".getBytes(),
        createdTs.plusSeconds(8),
        None,
      ),
      OutboxRecord(
        RecordId(5),
        "abcxy".getBytes(),
        createdTs.plusSeconds(16),
        None,
      ),
    )
    val dao  = TestOutboxRecordDAO(unpublishedRecords)
    val prod = TestOutboxKafkaProducer()

    given OutboxPublisherConfig = OutboxPublisherConfig(
      rate = 5.seconds,
      maxMessages = 100,
      publisherParallelism = 1,
      writebackChunkSize = 20,
      writebackMaxDelay = 200.millis
    )
    given (OutboxRecord => ProducerRecord[Option[String], Array[Byte]]) =
      record => ProducerRecord("nopic", None, record.message)

    val publisher = OutboxPublisher(dao, prod)

    assume(dao.Counts.unpublishedStream == 0)
    assume(dao.Counts.setPublished == 0)
    assume(prod.Counts.produce == 0)

    for {
      killswitch <- publisher.run
      _          <- IO.sleep(2.second)
      _ = assert(dao.Counts.unpublishedStream == 1)
      _ = assert(dao.Counts.setPublished > 0)
      _ = assert(prod.Counts.produce == 5)
      outcome <- killswitch.term()
    } yield assertEquals(outcome, Succeeded(IO.unit))
  }
}
