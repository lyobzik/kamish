package main

import com.typesafe.scalalogging.LazyLogging

object Kamish extends App with LazyLogging {
  case class Arguments(config: String = "/etc/kamish.conf")

  val parser = new scopt.OptionParser[Arguments]("kamish") {
    opt[String]('c', "config").action((x, c) => c.copy(config = x)).text("path to config file")
  }

  parser.parse(args, Arguments()) match {
    case Some(arguments) =>
      val config = new Config(arguments.config)
      logger.info(s"run with args: ${arguments.toString}")
      logger.info(s"run with config: $config")

      val workThread = spawn {
        val relayer = new Relayer(config)
        relayer.work()
        relayer.close()
      }
      sys.addShutdownHook({
        shutdown(config, workThread)
      })

    case None =>
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