package main

import java.util.Properties

import kafka.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
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

object HelloWorld extends App {
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

      println("consumer: " + consumer)
      println("run with args: " + arguments.toString)
      println("run with config: " + consumerConfig)

      try {
        while (true) {
          val records = consumer.poll(1000)
          println("subscription: " + consumer.subscription())
          println("assignment: " + consumer.assignment())
          if (records.nonEmpty) {
            println("Read " + records.size + " records")
          }
          records.foreach((record: ConsumerRecord[String, String]) =>
            println("Received record:\n  " + record.partition() + " - " +
              record.offset() + ": " + record.value()))
        }
      } catch {
        case err: Exception => println("Error: " + err)
      } finally {
        consumer.close()
      }

    case None =>
  }
}