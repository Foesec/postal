package li.flxkbr.postal.testkit

import li.flxkbr.postal.db.dao.OutboxRecordDAO
import li.flxkbr.postal.db.RecordId
import java.time.Instant
import li.flxkbr.postal.db.OutboxRecord
import cats.data.NonEmptyList
import cats.effect.IO
import fs2.Stream

class TestOutboxRecordDAO(unpublishedRecords: Seq[OutboxRecord])
    extends OutboxRecordDAO {

  object Counts {
    var unpublishedStream: Int = 0
    var setPublished: Int      = 0
  }

  override def unpublishedStream: Stream[IO, OutboxRecord] = {
    Counts.unpublishedStream = Counts.unpublishedStream + 1
    if Counts.unpublishedStream == 1 then
      Stream.emits(unpublishedRecords).covary[IO]
    else Stream.empty.covary[IO]
  }

  override def setPublished(
      ids: NonEmptyList[RecordId],
      publishedTs: Instant,
  ): IO[Int] = {
    println("Calling setPublished")
    Counts.setPublished = Counts.setPublished + 1
    IO(ids.size)
  }

}
