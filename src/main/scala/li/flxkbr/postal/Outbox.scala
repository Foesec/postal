package li.flxkbr.postal

import doobie.*
import fs2.kafka.Serializer
import cats.effect.IO

trait Outbox {

  def commit[A, B : Write, F[_]](tx: ConnectionIO[A], msg: B)(using
      Serializer[F, B],
  ): F[Unit]
}
