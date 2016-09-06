package main

import collection.JavaConverters._

import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}

class Config(configPath: String) {
  import java.io.File
  import com.typesafe._

  private[this] val conf = config.ConfigFactory.parseFile(new File(configPath))

  // Let it crash in incorrect config
  val inputKafka = convertToMap(
    conf.getConfig("input.kafka").withFallback(defaultDeserializersConfig))
  val inputKafkaTopic = conf.getString("input.topic")

  val outputKafka = convertToMap(
    conf.getConfig("output.kafka").withFallback(defaultSerializersConfig))
  val outputKafkaTopic = conf.getString("output.topic")

  validate()

  override def toString =
    s"{$inputKafka, $inputKafkaTopic -> $outputKafka, $outputKafkaTopic}"

  private def convertToMap(config: com.typesafe.config.Config): Map[String, Object] = {
    config.entrySet.asScala.map(
      value => (value.getKey, value.getValue.unwrapped)).toMap
  }

  private def defaultDeserializersConfig = {
    val defaultDeserializers = Map(
      "key.deserializer" -> classOf[StringDeserializer].getName,
      "value.deserializer" -> classOf[StringDeserializer].getName)
    config.ConfigFactory.parseMap(defaultDeserializers.asJava)
  }

  private def defaultSerializersConfig = {
    val defaultSerializers = Map(
      "value.serializer" -> classOf[StringSerializer].getName)
    config.ConfigFactory.parseMap(defaultSerializers.asJava)
  }

  private[this] def validate(): Unit = {
    mustContains(inputKafka, "group.id", "Input")
    mustContains(inputKafka, "bootstrap.servers", "Input")

    mustContains(outputKafka, "bootstrap.servers", "Output")
    mustContains(outputKafka, "key.serializer", "Output")

    if (inputKafkaTopic.isEmpty) throw new Exception("Input topic is required")
    if (outputKafkaTopic.isEmpty) throw new Exception("Output topic is required")

    if (inputKafka("group.id").asInstanceOf[String].isEmpty) {
      throw new Exception("Input group.id is required")
    }
  }

  private[this] def mustContains(config: Map[String, Object], key: String, title: String): Unit = {
    if (!config.contains(key)) {
      throw new Exception(s"$title $key is required")
    }
  }
}
