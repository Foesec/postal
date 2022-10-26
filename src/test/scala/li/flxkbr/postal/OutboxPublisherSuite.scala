package li.flxkbr.postal

import cats.effect.IO
import cats.effect.kernel.Outcome.*
import fs2.kafka.{KafkaProducer, ProducerRecord}
import li.flxkbr.postal.config.OutboxPublisherConfig
import li.flxkbr.postal.db.dao.OutboxRecordDAO
import li.flxkbr.postal.db.{OutboxRecord, RecordId}
import li.flxkbr.postal.log.DefaultIOLogging
import li.flxkbr.postal.testkit.*

import java.time.Instant
import scala.concurrent.duration.DurationInt

class OutboxPublisherSuite extends munit.CatsEffectSuite with DefaultIOLogging {

  import org.legogroup.woof.given_LogInfo

  val createdTs = Instant.parse("2007-12-03T10:15:30.00Z")

  val unpublishedRecords = Seq(
    OutboxRecord(
      RecordId(1),
      "nopic",
      None,
      "hello".getBytes(),
      createdTs,
      None,
    ),
    OutboxRecord(
      RecordId(2),
      "nopic",
      None,
      "goodb".getBytes(),
      createdTs.plusSeconds(2),
      None,
    ),
    OutboxRecord(
      RecordId(3),
      "nopic",
      None,
      "12345".getBytes(),
      createdTs.plusSeconds(4),
      None,
    ),
    OutboxRecord(
      RecordId(4),
      "nopic",
      None,
      "xyzab".getBytes(),
      createdTs.plusSeconds(8),
      None,
    ),
    OutboxRecord(
      RecordId(5),
      "nopic",
      None,
      "abcxy".getBytes(),
      createdTs.plusSeconds(16),
      None,
    ),
  )

  given (OutboxRecord => ProducerRecord[Option[Array[Byte]], Array[Byte]]) =
    record => ProducerRecord(record.topic, record.key, record.value)

  given OutboxPublisherConfig = OutboxPublisherConfig(
    rate = 2.seconds,
    maxMessages = 100,
    publisherParallelism = 1,
    publishBatchSize = 20,
    publishMaxDelay = 50.millis,
  )

  test("outbox publisher correctly runs") {

    val dao  = TestOutboxRecordDAO(unpublishedRecords)
    val prod = TestOutboxKafkaProducer()

    val publisher = OutboxPublisher(dao, prod)

    assume(dao.Counts.unpublishedStream == 0)
    assume(dao.Counts.setPublished == 0)
    assume(prod.Counts.produce == 0)
    assume(prod.Counts.producedRecords == 0)

    for {
      killswitch <- publisher.run
      _          <- IO.sleep(1.second)
      _ = assert(dao.Counts.unpublishedStream == 1)
      _ = assert(dao.Counts.setPublished > 0)
      _ = assert(prod.Counts.produce == 1)
      _ = assert(prod.Counts.producedRecords == 5)
      outcome <- killswitch.kill(5.seconds)
    } yield assertEquals(outcome, Succeeded(IO.unit))
  }

  test("outbox publisher handles failed publishing") {

    given OutboxPublisherConfig = OutboxPublisherConfig(
      rate = 500.millis,
      maxMessages = 100,
      publisherParallelism = 1,
      publishBatchSize = 20,
      publishMaxDelay = 100.millis,
    )

    val dao  = TestOutboxRecordDAO(unpublishedRecords)
    val prod = TestOutboxKafkaProducer(_ == 1) // fail the first invocation

    val publisher = OutboxPublisher(dao, prod)

    assume(dao.Counts.unpublishedStream == 0)
    assume(dao.Counts.setPublished == 0)
    assume(prod.Counts.produce == 0)

    fail("todo")
  }
}
