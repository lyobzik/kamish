package main

import collection.JavaConversions._

import com.typesafe.scalalogging.LazyLogging

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition

class Relayer(private val config: Config) extends LazyLogging {
  private type Key = String
  private type ProducerRecords[K, V] = List[ProducerRecord[K, V]]

  // TODO: хорошо бы научиться менять тип ключа producer'а на основании key.serializer в конфиге.
  private[this] val producer = new KafkaProducer[Key, String](config.outputKafka)
  private[this] val consumer = new KafkaConsumer[String, String](config.inputKafka)
  private[this] val rebalancedCallback = new RebalancedCallback(consumer,
    config.outputKafkaTopic, config.inputKafka, config.outputKafka)

  private[this] var assignment: java.util.Set[TopicPartition] = _

  def work(): Unit = {
    consumer.subscribe(List(config.inputKafkaTopic), rebalancedCallback)

    try {
      while (!Thread.interrupted) {
        val inRecords = read()
        val outRecords = convertRecords(inRecords)
        write(inRecords, outRecords)
      }
    } catch {
      // TODO: сделать обработку ошибок.
      case err: Exception => logger.error(s"Error: $err")
    }

    logger.info("Work thread stopped")
  }

  def close(): Unit = {
    consumer.close()
    producer.close()
  }

  private[this] def read(): ConsumerRecords[String, String] = {
    val inRecords = consumer.poll(config.pollTimeout.toMillis)
    if (assignment != consumer.assignment) {
      logger.info(s"Consumer assignment changed from $assignment to ${consumer.assignment}")
      assignment = consumer.assignment
    }
    if (inRecords.nonEmpty) {
      logger.debug(s"Read ${inRecords.size} records")
    }
    inRecords
  }

  private[this] def write(inRecords: ConsumerRecords[String, String],
                          outRecords: ProducerRecords[Key, String]): Unit = {
    val results = outRecords.map(record => producer.send(record))
    if (results.nonEmpty) {
      logger.info(s"Results: $results")
    }
    inRecords.zip(results).foreach{case(record, result) =>
      try {
        val _ = result.get()
        // TODO: обрабатывать ошибки отправки сообщений, понять посылает ли producer сообщения
        // повторно сам или это нужно делать вручную.
        val partition = new TopicPartition(record.topic, record.partition)
        val offset = new OffsetAndMetadata(record.offset + 1)
        consumer.commitSync(Map(partition -> offset))

        logger.debug(s"Commit $offset for partition $partition")
      } catch {
        case err: Exception =>
          logger.error(s"""Cannot produce message from (${record.topic}, ${record.partition}) with
            | offset [${record.offset}]: '${record.value}'. Error: ${err.getMessage}""".
            stripMargin.replaceAll("\n", " "))
      }
    }
  }

  private[this] def convertRecords(inRecords: ConsumerRecords[String, String]):
    ProducerRecords[Key, String] = {

    // TODO: добавить настраиваемое смещение key относительно исходного offset'а.
    inRecords.toList.map(record => new ProducerRecord[Key, String](config.outputKafkaTopic,
      record.partition, record.offset.toString, record.value))
  }
}