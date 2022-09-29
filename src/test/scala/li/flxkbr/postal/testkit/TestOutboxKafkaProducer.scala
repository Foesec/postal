package li.flxkbr.postal.testkit

import fs2.kafka.KafkaProducer
import cats.effect.IO
import fs2.kafka.ProducerRecords
import fs2.kafka.ProducerResult
import org.apache.kafka.clients.producer.internals.ProducerMetadata
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import scala.util.Random

class TestOutboxKafkaProducer()
    extends KafkaProducer[IO, Option[String], Array[Byte]] {

  object Counts {
    var produce: Int = 0
  }

  override def produce[P](
      records: ProducerRecords[P, Option[String], Array[Byte]],
  ): IO[IO[ProducerResult[P, Option[String], Array[Byte]]]] = {
    Counts.produce = Counts.produce + 1
    val prs = records.records.map { record =>
      (
        record,
        new RecordMetadata(
          TopicPartition("", 0),
          -1,
          0,
          record.timestamp.getOrElse(0),
          Random.nextLong(),
          record.key.map(_.size).getOrElse(0),
          record.value.size,
        ),
      )
    }
    IO(IO(ProducerResult(prs, records.passthrough)))
  }

}
