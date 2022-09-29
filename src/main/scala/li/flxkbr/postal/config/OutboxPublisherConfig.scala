package li.flxkbr.postal.config

import scala.concurrent.duration.FiniteDuration

final case class OutboxPublisherConfig(
    rate: FiniteDuration,
    maxMessages: Int,
    publisherParallelism: Int,
)
