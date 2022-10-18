package li.flxkbr.postal

import java.time.Instant

import li.flxkbr.postal.config.OutboxConfig
import li.flxkbr.postal.db.*
import li.flxkbr.postal.db.dao.OutboxRecordDAO
import cats.effect.{IO, MonadCancelThrow}
import cats.free.Free
import cats.implicits.{toFlatMapOps, toFunctorOps}
import cats.syntax.all.toTraverseOps
import doobie.free.connection.ConnectionIO
import doobie.implicits.*
import doobie.util.Write
import doobie.util.transactor.Transactor
import fs2.kafka.{Headers, Serializer}

class OutboxImpl[F[_]: MonadCancelThrow](xa: Transactor[F], outboxRecordDao: OutboxRecordDAO)(using
    cfg: OutboxConfig,
) {

  def commit[A, K, V](
      write: ConnectionIO[A],
      topic: String,
      key: Option[K],
      value: V,
      headers: Headers = Headers.empty,
  )(using
      kser: Serializer[F, K],
      vser: Serializer[F, V],
  ): F[A] = {
    for {
      key   <- key.map(kser.serialize(topic, headers, _)).sequence
      value <- vser.serialize(topic, headers, value)
      record = OutboxRecord.makeNew(topic, key, value, Instant.now())
      businessRes <- {
        for {
          businessRes <- write
          recordId    <- record.writeAutoInc.run
        } yield (businessRes)
      }.transact(xa)
    } yield (businessRes)
  }

}
