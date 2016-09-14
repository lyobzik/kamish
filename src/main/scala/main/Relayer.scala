package main

import collection.JavaConversions._

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition

class Record(outputTopic: String, val inRecord: ConsumerRecord[String, String]) {
  val outRecord = new ProducerRecord[String, String](outputTopic, inRecord.partition,
    inRecord.offset.toString, inRecord.value)
}

class Relayer(private val config: Config) extends LazyLogging {
  def work(): Unit = {
    val consumer = new KafkaConsumer[String, String](config.inputKafka)
    val rebalanceListener = new RebalancedCallback(consumer, config.outputKafkaTopic,
      config.inputKafka, config.outputKafka)
    consumer.subscribe(List(config.inputKafkaTopic), rebalanceListener)
    println("subscription: " + consumer.subscription())
    println("assignment: " + consumer.assignment())

    // TODO: хорошо бы научиться менять тип ключа producer'а на основании key.serializer в конфиге.
    val producer = new KafkaProducer[String, String](config.outputKafka)

    logger.info(s"consumer: $consumer")

    try {
      while (!Thread.interrupted) {
        val records = consumer.poll(config.pollTimeout.toMillis)
        logger.debug(s"subscription: ${consumer.subscription}")
        logger.debug(s"assignment: ${consumer.assignment}")
        if (records.nonEmpty) {
          logger.info(s"Read ${records.size} records")
        }
        // TODO: добавить настраиваемое смещение key относительно исходного offset'а.
        val committers = records.toList.map(new Record(config.outputKafkaTopic, _))
        val results = committers.map(c => producer.send(c.outRecord))
        committers.zip(results).foreach{case(commiter, result) =>
          // TODO: обрабатывать ошибки отправки сообщений, понять посылает ли producer сообщения
          // повторно сам или это нужно делать вручную.
          val partition = new TopicPartition(commiter.inRecord.topic, commiter.inRecord.partition)
          val offset = new OffsetAndMetadata(commiter.inRecord.offset + 1)
          consumer.commitSync(Map(partition -> offset))
          logger.info(s"Commit $offset for partition $partition")
        }
      }
    } catch {
      // TODO: сделать обработку ошибок.
      case err: Exception => logger.error(s"Error: $err")
    } finally {
      consumer.close()
      producer.close()
    }
    logger.info("Work thread stopped")
  }
}