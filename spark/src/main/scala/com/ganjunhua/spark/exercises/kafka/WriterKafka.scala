package com.ganjunhua.spark.exercises.kafka

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

import scala.util.Random

object WriterKafka {
  val random = new Random()
  var pointer = 1

  def getUserID(): String = {
    pointer += 1
    if (pointer >= KafkaProp.Users.length) {
      pointer = 0
      KafkaProp.Users(pointer)
    } else {
      KafkaProp.Users(pointer)
    }
  }

  def getOSType(): String = {
    if (pointer / 2 == 0) {
      "linux"
    } else {
      "windows"
    }
  }

  def click(): Double = {
    random.nextInt(10)
  }

  def main(args: Array[String]): Unit = {
    val topic = KafkaProp.Topic
    val props = new Properties()
    props.put(KafkaProp.MetadataBrokerList, KafkaProp.Brokers)
    props.put(KafkaProp.SerializerClass, KafkaProp.KafkaSerilizerStringEncoder)

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)

    while (true) {
      val event = new JSONObject()
      event.put("uid", getUserID())
      event.put("event_time", System.currentTimeMillis().toString)
      event.put("os_type", getOSType())
      event.put("click_count", click())

      val message = new KeyedMessage[String, String](topic, event.toString)
      producer.send(message)
      println("Message  sent:" + event)
      Thread.sleep(10000)
    }
  }
}
