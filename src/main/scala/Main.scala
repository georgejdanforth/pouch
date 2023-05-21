package pouch

import scala.io.StdIn

object Main {
  def main(args: Array[String]): Unit = {
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
}
