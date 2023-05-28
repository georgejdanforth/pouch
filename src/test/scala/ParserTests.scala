package pouch

import java.util.Arrays
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.PrivateMethodTester

class ParserTests extends AnyFunSuite with PrivateMethodTester {

  test("tokenize cleans whitespace correctly") {
    val tokenize = PrivateMethod[Array[Token]]('tokenize)
    val testCases = List("", " ", "     ", "  foo  ")
    testCases.foreach { t =>
      val result = Parser invokePrivate tokenize(t)
      assert(result.length == 1)
      assert(result(0).tokenType == TokenType.Err)
    }
  }

  test("tokenize is case insensitive") {
    val tokenize = PrivateMethod[Array[Token]]('tokenize)
    val testCases = List(
      ("q", TokenType.Quit),
      ("Q", TokenType.Quit),
      ("Quit", TokenType.Quit),
      ("QuIt", TokenType.Quit),
      ("QUIT", TokenType.Quit),
      ("get", TokenType.Get),
      ("GET", TokenType.Get),
      ("gET", TokenType.Get),
      ("set", TokenType.Set),
      ("Set", TokenType.Set),
      ("SET", TokenType.Set),
      ("del", TokenType.Del),
      ("dEl", TokenType.Del),
      ("DEL", TokenType.Del),
    )
    testCases.foreach {
      case (t, expected) => {
        val result = Parser invokePrivate tokenize(t)
        assert(result.length == 1)
        assert(result(0).tokenType == expected)
      }
    }
  }

  test("tokenize only emits args past first token") {
    val tokenize = PrivateMethod[Array[Token]]('tokenize)
    val testCases = List(
      ("q q", List(TokenType.Quit, TokenType.Arg)),
      ("quit quit", List(TokenType.Quit, TokenType.Arg)),
      ("get get", List(TokenType.Get, TokenType.Arg)),
      ("set set", List(TokenType.Set, TokenType.Arg)),
      ("del del", List(TokenType.Del, TokenType.Arg)),
    )
    testCases.foreach {
      case (t, expected) => {
        val result = Parser invokePrivate tokenize(t)
        result.zip(expected).foreach {
          case (r, e) => assert(r.tokenType == e)
        }
      }
    }
  }

  test("tokenize emits err for unrecognized command") {
    val tokenize = PrivateMethod[Array[Token]]('tokenize)
    val result = Parser invokePrivate tokenize("update key val")
    assert(result(0).tokenType == TokenType.Err)
  }

  test("parse unrecognized command") {
    val result = Parser.parse("foo bar")
    assert(result.isInstanceOf[ErrCommand])
    assert(result.asInstanceOf[ErrCommand].errType == ParseErrType.UnrecognizedCommand)
  }

  test("parse quit") {
    val testCases = List("q", "Q", "QUIT", "quit", "QuIt", "quit foo bar")
    testCases.foreach { t =>
      val result = Parser.parse(t)
      assert(result.isInstanceOf[QuitCommand])
    }
  }

  test("parse get ok") {
    val testCases = List(
      ("get foo", "foo"),
      ("GET foo", "foo"),
      ("GeT foo", "foo"),
    )
    testCases.foreach {
      case (t, expected) => {
        val result = Parser.parse(t)
        assert(result.isInstanceOf[GetCommand])
        assert(Arrays.equals(result.asInstanceOf[GetCommand].key, expected.getBytes))
      }
    }
  }

  test("parse get error") {
    val testCases = List("get foo bar", "get foo bar baz", "get")
    testCases.foreach { t =>
      val result = Parser.parse(t)
      assert(result.isInstanceOf[ErrCommand])
      assert(result.asInstanceOf[ErrCommand].errType == ParseErrType.InvalidArgs)
    }
  }

  test("parse set ok") {
    val testCases = List(
      ("set foo bar", "foo", "bar"),
      ("SET foo bar", "foo", "bar"),
      ("SeT foo bar", "foo", "bar"),
    )
    testCases.foreach {
      case (t, expectedKey, expectedValue) => {
        val result = Parser.parse(t)
        assert(result.isInstanceOf[SetCommand])
        assert(Arrays.equals(result.asInstanceOf[SetCommand].key, expectedKey.getBytes))
        assert(Arrays.equals(result.asInstanceOf[SetCommand].value, expectedValue.getBytes))
      }
    }
  }

  test("parse set error") {
    val testCases = List("set foo", "set foo bar baz", "get")
    testCases.foreach { t =>
      val result = Parser.parse(t)
      assert(result.isInstanceOf[ErrCommand])
      assert(result.asInstanceOf[ErrCommand].errType == ParseErrType.InvalidArgs)
    }
  }

  test("parse del ok") {
    val testCases = List(
      ("del foo", "foo"),
      ("DEL foo", "foo"),
      ("DeL foo", "foo"),
    )
    testCases.foreach {
      case (t, expected) => {
        val result = Parser.parse(t)
        assert(result.isInstanceOf[DelCommand])
        assert(Arrays.equals(result.asInstanceOf[DelCommand].key, expected.getBytes))
      }
    }
  }

  test("parse del error") {
    val testCases = List("del foo bar", "del foo bar baz", "del")
    testCases.foreach { t =>
      val result = Parser.parse(t)
      assert(result.isInstanceOf[ErrCommand])
      assert(result.asInstanceOf[ErrCommand].errType == ParseErrType.InvalidArgs)
    }
  }
}
