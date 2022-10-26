package li.flxkbr.postal.testkit

import scala.util.Random

import li.flxkbr.postal.OutboxKafkaProducer
import li.flxkbr.postal.db.RecordId
import cats.effect.IO
import fs2.kafka.{KafkaProducer, ProducerRecords, ProducerResult}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.internals.ProducerMetadata
import org.apache.kafka.common.TopicPartition

class TestOutboxKafkaProducer(failingInvocation: Int => Boolean = _ => false)
    extends OutboxKafkaProducer {

  object Counts {
    var produce: Int         = 0
    var producedRecords: Int = 0
  }

  override def produce[P](
      records: ProducerRecords[P, Option[Array[Byte]], Array[Byte]],
  ): IO[IO[ProducerResult[P, Option[Array[Byte]], Array[Byte]]]] = {
    Counts.produce = Counts.produce + 1
    println(s"Calling OutboxKafkaProducer.produce, count ${Counts.produce}")
    if failingInvocation(Counts.produce) then IO.raiseError(Exception("boom"))
    else
      Counts.producedRecords = Counts.producedRecords + records.records.size
      val prs = records.records.map { record =>
        (
          record,
          new RecordMetadata(
            TopicPartition("", 0),
            -1,
            0,
            record.timestamp.getOrElse(0),
            Random.nextLong(),
            record.key.map(_.length).getOrElse(0),
            record.value.length,
          ),
        )
      }
      IO.pure(IO.pure(ProducerResult(prs, records.passthrough)))
  }

}
