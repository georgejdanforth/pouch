package pouch

import java.nio.ByteBuffer

final object RecordType {
  val Data = 'D'
  val Tombstone = 'T'
}

final case class Record(recordType: Char, key: Array[Byte], value: Option[Array[Byte]])

object Encoder {
  private final val baseRecordSize = 2 * java.lang.Integer.BYTES + java.lang.Character.BYTES

  def getRecordSize(key: Array[Byte], value: Option[Array[Byte]] = None): Int = {
    value match {
      case Some(value) => baseRecordSize + key.length + value.length
      case None => baseRecordSize + key.length
    }
  }

  def getRecordSize(record: Record): Int = getRecordSize(record.key, record.value)

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

  def encodeRecord(record: Record): Array[Byte] = encodeRecord(record.recordType, record.key, record.value)

  def decodeRecord(buffer: ByteBuffer): Record = {
    val recordSize = buffer.getInt
    val keySize = buffer.getInt
    val recordType = buffer.getChar
    val key = Array.ofDim[Byte](keySize)
    buffer.get(key)

    recordType match {
      case RecordType.Data => {
        val valueSize = recordSize - baseRecordSize - keySize
        val value = Array.ofDim[Byte](valueSize)
        buffer.get(value)
        return Record(recordType, key, Some(value))
      }
      case RecordType.Tombstone => {
        return Record(recordType, key, None)
      }
    }
  }
}
