import fs2.kafka.*
import doobie.*
import doobie.implicits.*
import cats.effect.*
import cats.effect.implicits.*

val settings = ProducerSettings[IO, Array[Byte], Array[Byte]]
  .withBootstrapServers("localhost:9092")

val prod = KafkaProducer.stream(settings)