package main

import java.util
import java.util.UUID

import collection.JavaConversions._

import com.typesafe.scalalogging.LazyLogging

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord,
  ConsumerRecords, KafkaConsumer}

class RebalancedCallback(private val consumer: KafkaConsumer[String, String],
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
