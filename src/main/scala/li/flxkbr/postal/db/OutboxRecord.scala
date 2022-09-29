package li.flxkbr.postal.db

import java.time.Instant

import li.flxkbr.postal.config.OutboxConfig
import cats.Show
import doobie.*
import doobie.implicits.toSqlInterpolator
import doobie.util.fragment.Fragment

final case class RecordId(value: Int)

object RecordId {
  val Zero = RecordId(0)
}

final case class OutboxRecord(
    id: RecordId,
    message: Array[Byte],
    createdTs: Instant,
    publishedTs: Option[Instant],
) {

  def writeAutoInc(using cfg: OutboxConfig): Update0 = {
    sql"INSERT INTO ${cfg.outboxTableName}(message, created_ts, published_ts) VALUES $this".update
  }
}

object OutboxRecord {

  given Show[OutboxRecord] = Show
    .show(rec => s"{${rec.id}: ${rec.message}, ${rec.createdTs.getEpochSecond}, ${if rec.publishedTs.isDefined then "published" else "unpublished"}}")

  given writeAutoId: Write[OutboxRecord] =
    Write[(Array[Byte], Instant, Option[Instant])].contramap(r =>
      (r.message, r.createdTs, r.publishedTs),
    )

  def makeNew(message: Array[Byte], createdTs: Instant): OutboxRecord =
    OutboxRecord(RecordId.Zero, message, createdTs, None)
}
