package main


case class Arguments(config: String = "/etc/kamish.conf")

class Config(configPath: String) {
  import java.io.File
  import com.typesafe._
  private[this] val conf = config.ConfigFactory.parseFile(new File(configPath))

  // Let it crash
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
      println("run with args: " + arguments.toString)
      println("run with config: " + config.toString)
    case None =>
  }
}