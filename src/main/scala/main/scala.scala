package main

import java.util
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import collection.JavaConversions._

case class Arguments(config: String = "/etc/kamish.conf")

class OnConsumerRebalance(private val consumer: KafkaConsumer[String, String],
                          private val topic: String,
                          private val inputKafkaConfig: Map[String, Object],
                          private val outputKafkaConfig: Map[String, Object])
  extends ConsumerRebalanceListener with LazyLogging {

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
    logger.info(s"Partitions $partitions are revoked")
  }

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    logger.info(s"Partitions $partitions are assigned")

    val (emptyPartitions, positions) = getStartPositions(partitions)
    val isEmpty = (part: TopicPartition) => emptyPartitions contains part.partition

    logger.info(s"Seek to '$emptyPartitions', '$positions'")
    consumer.seekToBeginning(partitions.filter(isEmpty))
    partitions.filterNot(isEmpty).foreach(
      partition => consumer.seek(partition, positions(partition.partition))
    )
  }

  private[this] def getStartPositions(inPartitions: util.Collection[TopicPartition]): (
    List[Int], Map[Int, Long]) = {

    val consumer = new KafkaConsumer[String, String](
      outputConsumerConfig(inputKafkaConfig, outputKafkaConfig))

    // Seek output consumer to last message in partitions.
    val partitions = inPartitions.map(part => new TopicPartition(topic, part.partition))
    consumer.assign(partitions)
    consumer.seekToEnd(partitions)

    // Output topic may been in one of three states:
    // 1. Output topic is empty and never contained any messages => start from beginning.
    // 2. Output topic contains a message => (key of last message + 1) is start position.
    // 3. Output topic is empty, but some time ago already had contained messages => get start position
    //    from another storage or start from beginning.
    // TODO: кидается ли исключение на пустой партиции.
    val positions = partitions.map(part => consumer.position(part) - 1).toList
    val (emptyPartitions, nonemptyPartitions) =
      partitions.zip(positions).partition{case (_, position) => position < 0}

    nonemptyPartitions.foreach{case (partition, position) => consumer.seek(partition, position)}
    val records = readAll(consumer)

    consumer.close()

    (emptyPartitions.map(_._1.partition).toList,
      records.map(record => record.partition -> (record.key.toLong + 1)).toMap)
  }

  private[this] def readAll(consumer: KafkaConsumer[String, String]): List[ConsumerRecord[String, String]] = {
    var result = List[ConsumerRecord[String, String]]()
    var records: ConsumerRecords[String, String] = null
    while (records == null || records.nonEmpty) {
      records = consumer.poll(1000)
      result = result ++ records
    }
    result
  }

  private[this] def outputConsumerConfig(inConfig: Map[String, Object],
                                         outConfig: Map[String, Object]): Map[String, Object] = {
    inConfig + ("bootstrap.servers" -> outConfig("bootstrap.servers"),
      "enable.auto.commit" -> "false",
      "group.id" -> UUID.randomUUID.toString,
      "key.deserializer" -> outConfig("key.serializer").toString.replace("Serializer", "Deserializer"),
      "value.deserializer" -> outConfig("value.serializer").toString.replace("Serializer", "Deserializer"))
  }
}

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

      val workThread = spawn {
        work(config)
      }
      sys.addShutdownHook({
        shutdown(config, workThread)
      })

    case None =>
  }

  private[this] def work(config: Config): Unit = {
    val consumer = new KafkaConsumer[String, String](config.inputKafka)
    val rebalanceListener = new OnConsumerRebalance(consumer, config.outputKafkaTopic,
      config.inputKafka, config.outputKafka)
    consumer.subscribe(List(config.inputKafkaTopic), rebalanceListener)
    println("subscription: " + consumer.subscription())
    println("assignment: " + consumer.assignment())

    // TODO: хорошо бы научиться менять тип ключа producer'а на основании key.serializer в конфиге.
    val producer = new KafkaProducer[String, String](config.outputKafka)

    logger.info(s"consumer: $consumer")

    try {
      while (!Thread.currentThread.isInterrupted) {
        val records = consumer.poll(config.pollTimeout.toMillis)
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
      producer.close()
    }
    logger.info("Work thread stopped")
  }

  private[this] def shutdown(config: Config, thread: Thread): Unit = {
    if (thread != null) {
      thread.interrupt()
      thread.join(config.shutdownTimeout.toMillis)
      logger.info("Stopped")
    }
  }

  private[this] def spawn(function: => Unit): Thread = {
    val thread = new Thread(new Runnable {
      override def run(): Unit = function
    })
    thread.start()
    thread
  }
}