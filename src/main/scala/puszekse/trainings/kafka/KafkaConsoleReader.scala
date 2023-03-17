package puszekse.trainings.kafka

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}

import java.time.Duration

object KafkaConsoleReader {

  import java.util.Properties

  val TEST_TOPIC = "initial.test.topic"
  val TEST_GROUP_ID = "initial.group.id"

  val properties = new Properties
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:10000")
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, TEST_GROUP_ID)
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

  val kafkaConsumer: Consumer[String, String] = new KafkaConsumer[String, String](properties)

  var ENABLED = true

  val pollDuration = Duration.ofMillis(100L)


  def main(args: Array[String]): Unit = {
    import scala.jdk.CollectionConverters._

    kafkaConsumer.subscribe(List(TEST_TOPIC).asJava)
    while (ENABLED) {
      val records = kafkaConsumer.poll(pollDuration)

      for (record <- records.asScala){
        println(record)
      }
    }
  }

}
