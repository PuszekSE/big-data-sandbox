package puszekse.trainings.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import java.util.UUID
import scala.concurrent.duration.Duration

object KafkaGenerator {

  import java.util.Properties

  val TEST_TOPIC = "initial.test.topic"

  val properties = new Properties
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:10000")
  properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "producer.id")
  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0")
  properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Math.pow(2, 14).toLong.toString)
  properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, Math.pow(2, 20).toLong.toString)

  val kafkaProducer: Producer[String, String] = new KafkaProducer(properties)

  val sleepInterval = Duration("1 second")

  var ENABLED = true

  def main(args: Array[String]): Unit = {
    while (ENABLED) {
      kafkaProducer.send(generateRecord())
      Thread.sleep(sleepInterval.toMillis)
    }
  }

  private val loremIpsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."

  private def generateRecord(): ProducerRecord[String, String] = {
    val uuid = UUID.randomUUID().toString
    new ProducerRecord(TEST_TOPIC, uuid, EventData(uuid, loremIpsum, System.currentTimeMillis()).toString)
  }
}
