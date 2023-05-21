package pouch

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
  )

  val builder = OParser.builder[Config]
  val argParser = {
    import builder._
    OParser.sequence(
      programName("pouch"),
      head("pouch", "0.0.0"),
      cmd(RunMode.Init)
        .action((_, c) => c.copy(mode = RunMode.Init)),
      cmd(RunMode.CLI)
        .action((_, c) => c.copy(mode = RunMode.CLI)),
    )
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
          case RunMode.Init => println("init")
          case RunMode.CLI => cli()
          case _ => println(OParser.usage(argParser))
        }
      }
      case _ => sys.exit(1)
    }
  }
}
