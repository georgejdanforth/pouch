package pouch

import java.nio.ByteBuffer
import java.nio.file.{Files,Path,StandardOpenOption}

final object RecordType {
  val Data = 'D'
  val Tombstone = 'T'
}

class Executor(dataFile: Path) {
  val intBytes: Int = java.lang.Integer.BYTES
  val charBytes: Int = java.lang.Character.BYTES

  private def printBuffer(buf: ByteBuffer): Unit = {
    buf.array().foreach(c => print("%02x ".format(c)))
    print("\n --- \n")
  }

  private def encode(recordType: Char, key: String, value: String): Array[Byte] = {
    val keyBytes = key.getBytes()
    val valBytes = value.getBytes()

    val bufSize = 2 * intBytes + charBytes + keyBytes.length + valBytes.length
    val buffer = ByteBuffer.allocate(bufSize)

    buffer.putInt(bufSize)
    buffer.putInt(keyBytes.length)
    buffer.putChar(recordType)
    buffer.put(keyBytes)
    buffer.put(valBytes)

    return buffer.array()
  }

  def set(key: String, value: String): Unit = {
    val record = encode(RecordType.Data, key, value)
    Files.write(dataFile, record, StandardOpenOption.APPEND)
  }

  def delete(key: String): Unit = {
    val record = encode(RecordType.Tombstone, key, "")
    Files.write(dataFile, record, StandardOpenOption.APPEND)
  }
}
