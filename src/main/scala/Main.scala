package pouch

import java.nio.file.{Files,Path}
import scala.io.StdIn
import scala.sys

import scopt.OParser

object Main {

  object RunMode {
    final val Init = "init"
    final val CLI = "cli"
  }

  case class Config(
    mode: String = "",
    dataDir: Option[Path] = None,
  )

  val builder = OParser.builder[Config]
  val argParser = {
    import builder._
    OParser.sequence(
      programName("pouch"),
      head("pouch", "0.0.0"),
      cmd(RunMode.Init)
        .action((_, c) => c.copy(mode = RunMode.Init))
        .children(
          opt[Path]('D', "data-dir")
            .required()
            .action((path, c) => c.copy(dataDir = Some(path)))
            .text("Data directory"),
        ),
      cmd(RunMode.CLI)
        .action((_, c) => c.copy(mode = RunMode.CLI)),
    )
  }

  private def ensureDataDir(dataDir: Path): Unit = {
    if (!Files.exists(dataDir)) {
      try {
        Files.createDirectory(dataDir)
      } catch {
        case e: Exception => {
          println("Failed to create data directory: %s".format(e.getMessage()))
          sys.exit(1)
        }
      }
    } else if (!Files.isDirectory(dataDir)) {
      println("Path exists and is not a directory: %s".format(dataDir.toString()))
      sys.exit(1)
    }
    try {
      if (Files.list(dataDir).count() > 0) {
        println("Expected empty data directory: %s".format(dataDir.toString()))
        sys.exit(1)
      }
    } catch {
      case e: Exception => {
        println("Failed to read data directory: %s".format(e.getMessage()))
        sys.exit(1)
      }
    }
  }

  private def initDataFile(dataDir: Path): Unit = {
    val filePath = dataDir.resolve("pouch.db")
    if (Files.exists(filePath)) {
      println("Data file already exists: %s".format(filePath.toString()))
      sys.exit(1)
    }
    try {
      Files.createFile(filePath)
    } catch {
      case e: Exception => {
        println("Failed to create data file: %s".format(e.getMessage()))
        sys.exit(1)
      }
    }
  }

  private def init(dataDir: Option[Path]): Unit = {
    dataDir match {
      case Some(dataDir) => {
        ensureDataDir(dataDir)
        initDataFile(dataDir)
      }
      case None => {
        println("A valid filesystem path is required")
        sys.exit(1)
      }
    }
  }

  private def cli(): Unit = {
    while (true) {
      print("> ")
      val input = scala.io.StdIn.readLine()
      val cmd = Parser.parse(input)
      cmd match {
        case cmd : QuitCommand => return
        case cmd : GetCommand => println("GET %s".format(cmd.key))
        case cmd : SetCommand => println("SET %s %s".format(cmd.key, cmd.value))
        case cmd : DelCommand => println("DEL %s".format(cmd.key))
        case cmd : ErrCommand => println("Error: %s".format(cmd.message))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    OParser.parse(argParser, args, Config()) match {
      case Some(config) => {
        config.mode match {
          case RunMode.Init => init(config.dataDir)
          case RunMode.CLI => cli()
          case _ => println(OParser.usage(argParser))
        }
      }
      case _ => sys.exit(1)
    }
  }
}
