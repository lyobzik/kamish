package main

import java.time.Duration

import collection.JavaConverters._

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

class Config(configPath: String) {
  import java.io.File
  import com.typesafe._

  private[this] val conf = config.ConfigFactory.parseFile(new File(configPath)).
    withFallback(defaultConfig)

  // Let it crash in incorrect config
  val inputKafka = convertToMap(conf.getConfig("input.kafka"))
  val inputKafkaTopic = conf.getString("input.topic")

  val outputKafka = convertToMap(conf.getConfig("output.kafka"))
  val outputKafkaTopic = conf.getString("output.topic")

  val shutdownTimeout = conf.getDuration("shutdownTimeout")
  val pollTimeout = conf.getDuration("input.pollTimeout")

  validate()

  override def toString =
    s"""{$inputKafka, $inputKafkaTopic -> $outputKafka, $outputKafkaTopic;
      |$shutdownTimeout, $pollTimeout}""".stripMargin.replaceAll("\n", " ")

  private def convertToMap(config: com.typesafe.config.Config): Map[String, Object] = {
    config.entrySet.asScala.map(
      value => (value.getKey, value.getValue.unwrapped)).toMap
  }

  private def defaultConfig = {
    val defaultSettings = Map(
      "input.kafka.key.deserializer" -> classOf[StringDeserializer].getName,
      "input.kafka.value.deserializer" -> classOf[StringDeserializer].getName,
      "output.kafka.value.serializer" -> classOf[StringSerializer].getName,

      "shutdownTimeout" -> Duration.ofMillis(1000),
      "input.pollTimeout" -> Duration.ofMillis(100)
    )
    config.ConfigFactory.parseMap(defaultSettings.asJava)
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
