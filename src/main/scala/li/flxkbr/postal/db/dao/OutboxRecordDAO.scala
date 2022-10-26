package li.flxkbr.postal.db.dao

import java.time.Instant

import li.flxkbr.postal.Outbox
import li.flxkbr.postal.config.OutboxConfig
import li.flxkbr.postal.db.{OutboxRecord, RecordId}
import cats.data.NonEmptyList
import cats.effect.*
import doobie.*
import doobie.implicits.{
  toConnectionIOOps,
  toDoobieStreamOps,
  toSqlInterpolator,
}
import fs2.*

trait OutboxRecordDAO {

  def insert(record: OutboxRecord): Update0

  def unpublishedStream: Stream[IO, OutboxRecord]

  def setPublished(
      ids: NonEmptyList[RecordId],
      publishedTs: Instant = Instant.now(),
  ): IO[Int]
}

private[dao] class OutboxRecordDAOImpl(xa: Transactor[IO])(using
    cfg: OutboxConfig,
) extends OutboxRecordDAO {

  override def insert(record: OutboxRecord): Update0 =
    sql"INSERT INTO ${cfg.outboxTableName}(topic, key, value, created_ts, published_ts) VALUES $record".update

  override lazy val unpublishedStream: Stream[IO, OutboxRecord] =
    sql"""SELECT * FROM ${cfg.outboxTableName}
          WHERE published_ts IS NULL"""
      .query[OutboxRecord]
      .stream
      .transact(xa)

  override def setPublished(
      ids: NonEmptyList[RecordId],
      publishedTs: Instant = Instant.now(),
  ): IO[Int] = (fr"""
    UPDATE ${cfg.outboxTableName} obx SET published_ts = now()
    WHERE """ ++ Fragments.in(fr"id", ids)).update.run.transact(xa)

}
