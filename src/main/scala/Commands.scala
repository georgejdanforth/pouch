package pouch

sealed trait Command

final case class QuitCommand() extends Command
final case class GetCommand(key: Array[Byte]) extends Command
final case class SetCommand(key: Array[Byte], value: Array[Byte]) extends Command
final case class DelCommand(key: Array[Byte]) extends Command
final case class ErrCommand(errType: ParseErrType.ParseErrType, message: String) extends Command
