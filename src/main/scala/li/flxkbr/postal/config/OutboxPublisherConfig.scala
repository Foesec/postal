package li.flxkbr.postal.config

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

final case class OutboxPublisherConfig(
    rate: FiniteDuration,
    maxMessages: Int,
    publisherParallelism: Int,
    writebackChunkSize: Int,
    writebackMaxDelay: FiniteDuration,
)

object OutboxPublisherConfig {
  val Default = OutboxPublisherConfig(
    rate = 20.seconds,
    maxMessages = 1_000,
    publisherParallelism = 1,
    writebackChunkSize = 20,
    writebackMaxDelay = 200.millis,
  )
}
