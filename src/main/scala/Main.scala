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
        .action((_, c) => c.copy(mode = RunMode.CLI))
        .children(
          opt[Path]('D', "data-dir")
            .required()
            .action((path, c) => c.copy(dataDir = Some(path)))
            .text("Data directory"),
        ),
    )
  }

  private def init(baseDataDir: Path): Unit = {
    val walDir = baseDataDir.resolve("wal")
    val dataDir = baseDataDir.resolve("data")

    // Create base data dir if not exists
    if (!Files.exists(baseDataDir)) {
      try {
        Files.createDirectory(baseDataDir)
      } catch {
        case e: Exception => {
          println("Failed to create base data directory: %s".format(e.getMessage()))
          sys.exit(1)
        }
      }
    } else if (!Files.isDirectory(baseDataDir)) {
      println("Path exists and is not a directory: %s".format(baseDataDir.toString()))
      sys.exit(1)
    }

    // Ensure base data directory is empty
    try {
      if (Files.list(baseDataDir).count() > 0) {
        println("Expected empty data directory: %s".format(baseDataDir.toString()))
        sys.exit(1)
      }
    } catch {
      case e: Exception => {
        println("Failed to read data directory: %s".format(e.getMessage()))
        sys.exit(1)
      }
    }

    // Create WAL directory
    try {
      Files.createDirectory(walDir)
    } catch {
      case e: Exception => {
        println("Failed to create WAL directory: %s".format(e.getMessage()))
        sys.exit(1)
      }
    }

    // Create initial WAL segment
    try {
      val walFilePath = walDir.resolve(Utils.intToSegmentNumber(0))
      Files.createFile(walFilePath)
    } catch {
      case e: Exception => {
        println("Failed to create WAL segment: %s".format(e.getMessage()))
        sys.exit(1)
      }
    }

    // Create data directory
    try {
      Files.createDirectory(dataDir)
    } catch {
      case e: Exception => {
        println("Failed to create data directory: %s".format(e.getMessage()))
        sys.exit(1)
      }
    }
  }

  private def validateDataDir(baseDataDir: Path): Unit = {
    val walDir = baseDataDir.resolve("wal")
    val dataDir = baseDataDir.resolve("data")
    if (!Files.isDirectory(walDir)) {
      println("Expected 'wal' subdirectory in data directory %s".format(baseDataDir.toString))
      sys.exit(1)
    }
    if (!Files.isDirectory(dataDir)) {
      println("Expected 'data' subdirectory in data directory %s".format(baseDataDir.toString))
      sys.exit(1)
    }
    if (Files.list(walDir).count() == 0) {
      println("Expected WAL directory %s to contain at least one WAL segment".format(walDir.toString))
      sys.exit(1)
    }
  }

  private def cli(baseDataDir: Path): Unit = {
    // Helper function to convert a byte array to a utf-8 encoded string
    val s = (a: Array[Byte]) => new String(a, "UTF-8")

    validateDataDir(baseDataDir)

    val executor = new Executor()
    while (true) {
      print("> ")
      val input = scala.io.StdIn.readLine()
      val cmd = Parser.parse(input)
      cmd match {
        case cmd : QuitCommand => return
        case cmd : GetCommand => {
          executor.get(cmd.key) match {
            case Some(value) => println(s(value))
            case None => println("(nil)")
          }
        }
        case cmd : SetCommand => {
          executor.set(cmd.key, cmd.value)
          println("SET %s %s".format(s(cmd.key), s(cmd.value)))
        }
        case cmd : DelCommand => {
          executor.delete(cmd.key)
          println("DEL %s".format(s(cmd.key)))
        }
        case cmd : ErrCommand => println("Error: %s".format(cmd.message))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    OParser.parse(argParser, args, Config()) match {
      case Some(config) => {
        config.dataDir match {
          case Some(dataDir) => {
            config.mode match {
              case RunMode.Init => init(config.dataDir.get)
              case RunMode.CLI => cli(config.dataDir.get)
              case _ => println(OParser.usage(argParser))
            }
          }
          case None => println(OParser.usage(argParser))
        }
      }
      case _ => sys.exit(1)
    }
  }
}
