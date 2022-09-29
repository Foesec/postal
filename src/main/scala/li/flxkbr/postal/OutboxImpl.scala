package li.flxkbr.postal

import java.time.Instant

import li.flxkbr.postal.config.OutboxConfig
import li.flxkbr.postal.db.*
import cats.free.Free
import doobie.free.connection.ConnectionIO
import doobie.util.Write
import fs2.kafka.{Headers, Serializer}

class OutboxImpl(using cfg: OutboxConfig) {

  def commit[A, B, F[_]](tx: ConnectionIO[A], topic: String, msg: B)(using
      se: Serializer[ConnectionIO, B],
  ): ConnectionIO[Unit] = {
    for {
      serializedMsg <- se.serialize(topic, Headers.empty, msg)
      record = OutboxRecord.makeNew(serializedMsg, Instant.now())
      businessWrite <- tx
      outboxWrite   <- record.writeAutoInc.run
    } yield ()
  }

}
