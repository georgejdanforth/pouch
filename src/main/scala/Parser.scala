package pouch

object ParseErrType extends Enumeration {
  type ParseErrType = Value

  val UnparseableInput, UnrecognizedCommand, InvalidArgs = Value
}

object Tokens {
  val Q = "q"
  val Quit = "quit"
  val Get = "get"
  val Set = "set"
  val Del = "del"
}

object TokenType extends Enumeration {
  type TokenType = Value

  val Quit, Get, Set, Del, Arg, Err = Value
}

case class Token(tokenType: TokenType.TokenType, value: String)

object Parser {
  private val cmdPattern = "^\\s*([\\w.-]+)\\s*([\\w.-]+)?\\s*$".r

  private def tokenize(input: String) = {
    input.trim.split("\\s+").zipWithIndex.map {
      case (word, i) => {
        val tokenType = i match {
          case 0 => {
            word.toLowerCase match {
              case Tokens.Q | Tokens.Quit => TokenType.Quit
              case Tokens.Get => TokenType.Get
              case Tokens.Set => TokenType.Set
              case Tokens.Del => TokenType.Del
              case _ => TokenType.Err
            }
          }
          case _ => TokenType.Arg
        }
        Token(tokenType, word)
      }
    }
  }

  def parse(cmd: String): Command = {
    val tokens = tokenize(cmd)
    if (tokens.length == 0) {
      return ErrCommand(ParseErrType.UnparseableInput, "Unparseable input")
    }
    tokens(0).tokenType match {
      case TokenType.Err | TokenType.Arg => ErrCommand(
        ParseErrType.UnrecognizedCommand,
        "Unrecognized command: %s".format(tokens(0).value)
      )
      case TokenType.Quit => QuitCommand()
      case TokenType.Get => {
        if (tokens.length != 2) {
          return ErrCommand(
            ParseErrType.InvalidArgs,
            "GET expects a single `key` argument. Got %d arguments".format(tokens.length - 1)
          )
        }
        return GetCommand(tokens(1).value.getBytes)
      }
      case TokenType.Set => {
        if (tokens.length != 3) {
          return ErrCommand(
            ParseErrType.InvalidArgs,
            "SET expects a `key` and a `value` argument. Got %d arguments".format(tokens.length - 1)
          )
        }
        return SetCommand(tokens(1).value.getBytes, tokens(2).value.getBytes)
      }
      case TokenType.Del => {
        if (tokens.length != 2) {
          return ErrCommand(
            ParseErrType.InvalidArgs,
            "DEL expects a single `key` argument. Got %d arguments".format(tokens.length - 1)
          )
        }
        return DelCommand(tokens(1).value.getBytes)
      }
    }
  }
}
