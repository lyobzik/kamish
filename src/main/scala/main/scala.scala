package main

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization._

import collection.JavaConversions._

case class Arguments(config: String = "/etc/kamish.conf")

class OnProduceMessage(private val value: String) extends Callback with LazyLogging {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    val exceptDesc = if (exception != null) exception.toString else  "NONE"
    logger.info(s"Produce record:  ${metadata.partition} - ${metadata.offset}: '$value' with exception $exceptDesc")
  }
}

object HelloWorld extends App with LazyLogging {
  val parser = new scopt.OptionParser[Arguments]("kamish") {
    opt[String]('c', "config").action((x, c) => c.copy(config = x)).text("path to config file")
  }

  parser.parse(args, Arguments()) match {
    case Some(arguments) =>
      val config = new Config(arguments.config)
      logger.info(s"run with args: ${arguments.toString}")
      logger.info(s"run with config: $config")

      // TODO: добавить обработчики ребалансировок, в которых бы определялся последний
      // записанные в outputTopic offset и этот offset устанавливался в consumer.
      val consumer = new KafkaConsumer[String, String](config.inputKafka)
      consumer.subscribe(List(config.inputKafkaTopic))
      println("subscription: " + consumer.subscription())
      println("assignment: " + consumer.assignment())
      consumer.seekToBeginning(consumer.assignment())

      // TODO: хорошо бы научиться менять тип ключа producer'а на основании key.serializer в конфиге.
      val producer = new KafkaProducer[String, String](config.outputKafka)

      logger.info(s"consumer: $consumer")

      try {
        while (true) {
          val records = consumer.poll(1000)
          logger.debug(s"subscription: ${consumer.subscription}")
          logger.debug(s"assignment: ${consumer.assignment}")
          if (records.nonEmpty) {
            logger.info(s"Read ${records.size} records")
          }
          records.foreach((record: ConsumerRecord[String, String]) => {
            // TODO: добавить настраиваемое смещение key относительно исходного offset'а.
            val outputRecord = new ProducerRecord[String, String](config.outputKafkaTopic, record.partition(),
              record.offset().toString, record.value())
            // TODO: обрабатывать ошибки отправки сообщений, понять посылает ли producer сообщения
            // повторно сам или это нужно делать вручную.
            producer.send(outputRecord, new OnProduceMessage(record.value))
          })
        }
      } catch {
        // TODO: сделать обработку ошибок.
        case err: Exception => logger.error(s"Error: $err")
      } finally {
        consumer.close()
      }

    case None =>
  }
}