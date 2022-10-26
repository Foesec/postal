package li.flxkbr.postal.db

import java.time.Instant
import li.flxkbr.postal.config.OutboxConfig
import cats.Show
import doobie.*
import doobie.implicits.toSqlInterpolator
import doobie.util.fragment.Fragment
import fs2.kafka.ProducerRecord

final case class RecordId(value: Int)

object RecordId {
  val Zero = RecordId(0)
}

final case class OutboxRecord(
    id: RecordId,
    topic: String,
    key: Option[Array[Byte]],
    value: Array[Byte],
    createdTs: Instant,
    publishedTs: Option[Instant],
) {
  def toProducerRecord: ProducerRecord[Option[Array[Byte]], Array[Byte]] =
    ProducerRecord(topic, key, value)
}

object OutboxRecord {

  given Show[OutboxRecord] = Show
    .show(rec =>
      s"{${rec.id}: ${rec.key}/${rec.value.mkString("Array(", ", ", ")")}, ${rec.createdTs.getEpochSecond}, " +
        s"${if rec.publishedTs.isDefined then "published" else "unpublished"}}",
    )

  given writeAutoId: Write[OutboxRecord] =
    Write[(String, Option[Array[Byte]], Array[Byte], Instant, Option[Instant])]
      .contramap(r => (r.topic, r.key, r.value, r.createdTs, r.publishedTs))

  def makeNew(
      topic: String,
      key: Option[Array[Byte]],
      value: Array[Byte],
      createdTs: Instant,
  ): OutboxRecord =
    OutboxRecord(RecordId.Zero, topic, key, value, createdTs, None)
}
