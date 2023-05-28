package pouch

import java.nio.ByteBuffer

final object RecordType {
  val Data = 'D'
  val Tombstone = 'T'
}

object Encoder {
  private final val baseRecordSize = 2 * java.lang.Integer.BYTES + java.lang.Character.BYTES

  def getRecordSize(key: Array[Byte], value: Option[Array[Byte]] = None): Int = {
    value match {
      case Some(value) => baseRecordSize + key.length + value.length
      case None => baseRecordSize + key.length
    }
  }

  def encodeRecord(
    recordType: Char,
    key: Array[Byte],
    value: Option[Array[Byte]],
  ): Array[Byte] = {
    val recordSize = getRecordSize(key, value)
    val buffer = ByteBuffer.allocate(recordSize)

    buffer.putInt(recordSize)
    buffer.putInt(key.length)
    buffer.putChar(recordType)
    buffer.put(key)
    value match {
      case Some(value) => buffer.put(value)
      case None =>
    }

    return buffer.array()
  }
}
