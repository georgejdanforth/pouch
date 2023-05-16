import scala.io.StdIn

object Main {
  def main(args: Array[String]): Unit = {
    while (true) {
      print("> ")
      val cmd = scala.io.StdIn.readLine()
      if (cmd == "q" || cmd == "quit") {
        return
      }
      println(cmd)
    }
  }
}
