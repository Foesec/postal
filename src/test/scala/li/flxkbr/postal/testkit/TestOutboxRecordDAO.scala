package li.flxkbr.postal.testkit

import li.flxkbr.postal.db.dao.OutboxRecordDAO
import li.flxkbr.postal.db.RecordId

import java.time.Instant
import li.flxkbr.postal.db.OutboxRecord
import cats.data.NonEmptyList
import cats.effect.IO
import doobie.Update0
import doobie.implicits.toSqlInterpolator
import fs2.Stream
import li.flxkbr.postal.log.DefaultIOLogging

class TestOutboxRecordDAO(unpublishedRecords: Seq[OutboxRecord])
    extends OutboxRecordDAO {

  object Counts {
    var unpublishedStream: Int = 0
    var setPublished: Int      = 0
  }

  override def insert(record: OutboxRecord): Update0 = sql"".update

  override def unpublishedStream: Stream[IO, OutboxRecord] = {
    Counts.unpublishedStream = Counts.unpublishedStream + 1
    println(s"Calling unpublishedStream, count ${Counts.unpublishedStream}")
    if Counts.unpublishedStream == 1 then
      Stream.emits(unpublishedRecords).covary[IO]
    else Stream.empty.covary[IO]
  }

  override def setPublished(
      ids: NonEmptyList[RecordId],
      publishedTs: Instant,
  ): IO[Int] = {
    Counts.setPublished = Counts.setPublished + 1
    println(s"Calling setPublished, count ${Counts.setPublished}")
    IO.pure(ids.size)
  }

}
