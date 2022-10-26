package li.flxkbr.postal.config

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

final case class OutboxPublisherConfig(
    rate: FiniteDuration,
    maxMessages: Int,
    publisherParallelism: Int,
    publishBatchSize: Int,
    publishMaxDelay: FiniteDuration,
)

object OutboxPublisherConfig {
  val Default: OutboxPublisherConfig = OutboxPublisherConfig(
    rate = 20.seconds,
    maxMessages = 1_000,
    publisherParallelism = 1,
    publishBatchSize = 20,
    publishMaxDelay = 200.millis,
  )
}
