package main

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization._

import collection.JavaConversions._

case class Arguments(config: String = "/etc/kamish.conf")

class Config(configPath: String) {
  import java.io.File
  import com.typesafe._
  private[this] val conf = config.ConfigFactory.parseFile(new File(configPath))

  // Let it crash in incorrect config
  val inputKafkaServers = conf.getStringList("input.kafka.servers")
  val inputKafkaTopic = conf.getString("input.kafka.topic")

  val outputKafkaServers = conf.getStringList("output.kafka.servers")
  val outputKafkaTopic = conf.getString("output.kafka.topic")

  override def toString = "{%s, %s -> %s, %s}".format(inputKafkaServers, inputKafkaTopic,
    outputKafkaServers, outputKafkaTopic)
}

class OnProduceMessage(private val value: String) extends Callback with LazyLogging {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    val exceptDesc = if (exception != null) exception.toString else  "NONE"
    logger.info(s"Produce record:  ${metadata.partition} - ${metadata.offset}: '$value' with exception $exceptDesc")
//    logger.info("Produce record:  " + metadata.partition() + " - " +
//      metadata.offset() + ": '" + value + "' with exception " + exceptDesc)
  }
}

object HelloWorld extends App with LazyLogging {
  val parser = new scopt.OptionParser[Arguments]("kamish") {
    opt[String]('c', "config").action((x, c) => c.copy(config = x)).text("path to config file")
  }

  parser.parse(args, Arguments()) match {
    case Some(arguments) =>
      val config = new Config(arguments.config)

      val consumerConfig = Map(
        "group.id" -> "kamish",
        "bootstrap.servers" -> config.inputKafkaServers,
        "key.deserializer" -> classOf[StringDeserializer].getName,
        "value.deserializer" -> classOf[StringDeserializer].getName)
      val consumer = new KafkaConsumer[String, String](consumerConfig)
      consumer.subscribe(List(config.inputKafkaTopic))
      println("subscription: " + consumer.subscription())
      println("assignment: " + consumer.assignment())
      consumer.seekToBeginning(consumer.assignment())

      val producerConfig = Map(
        "bootstrap.servers" -> config.outputKafkaServers,
        "key.serializer" -> classOf[StringSerializer].getName,
        "value.serializer" -> classOf[StringSerializer].getName)
      val producer = new KafkaProducer[String, String](producerConfig)

      logger.info(s"consumer: $consumer")
      logger.info(s"run with args: ${arguments.toString}")
      logger.info(s"run with config: $consumerConfig")

      try {
        while (true) {
          val records = consumer.poll(1000)
          logger.debug(s"subscription: ${consumer.subscription}")
          logger.debug(s"assignment: ${consumer.assignment}")
          if (records.nonEmpty) {
            logger.info(s"Read ${records.size} records")
          }
          records.foreach((record: ConsumerRecord[String, String]) => {
            val outputRecord = new ProducerRecord[String, String](config.outputKafkaTopic, record.partition(),
              record.offset().toString, record.value())
            producer.send(outputRecord, new OnProduceMessage(record.value))
          })
        }
      } catch {
        case err: Exception => logger.error(s"Error: $err")
      } finally {
        consumer.close()
      }

    case None =>
  }
}