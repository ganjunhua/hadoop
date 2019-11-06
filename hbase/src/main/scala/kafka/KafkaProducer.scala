package kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    println(classOf[StringSerializer].getName)
    producerMessage()
  }

  def producerMessage() = {
    val kafkaConf = defaultProperties()._1
    val produce = new KafkaProducer[String, String](kafkaConf)
    for (i <- 0 to 1000) {
      val key = i
      val value =  i
      val message = new ProducerRecord[String, String](defaultProperties()._2, key.toString, value.toString)
      println(key + "->" + value)
      produce.send(message)
    }

  }

  def defaultProperties() = {
    val topic = "kafka"
    val brokers = "holiday-f:9092,holiday-s:9092,holiday-t:9092"
    val prop = new Properties()
    prop.put("bootstrap.servers", brokers)
    prop.put("serializer.class", "kafka.serializer.StringEncoder")
    prop.setProperty("key.serializer", classOf[StringSerializer].getName)
    prop.setProperty("value.serializer", classOf[StringSerializer].getName)
    (prop, topic)
  }
}
