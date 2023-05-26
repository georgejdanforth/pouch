package pouch

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
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

  def get(key: String): Option[String] = {
    var value: Option[String] = None
    val chan = new RandomAccessFile(dataFile.toString(), "r").getChannel

    val sizeBuf = ByteBuffer.allocate(intBytes * 2)

    while (chan.position < chan.size) {
      chan.read(sizeBuf)
      sizeBuf.flip()
      val recordBytes = sizeBuf.getInt()
      val keyBytes = sizeBuf.getInt()
      val valBytes = recordBytes - 2 * intBytes - charBytes - keyBytes

      val recordBuf = ByteBuffer.allocate(recordBytes - 2 * intBytes)
      chan.read(recordBuf)
      recordBuf.flip()

      val recordType = recordBuf.getChar()
      recordBuf.limit(charBytes + keyBytes)
      val recordKey = StandardCharsets.UTF_8.decode(recordBuf).toString()

      if (recordKey == key) {
        if (recordType == RecordType.Tombstone) {
          value = None
        } else {
          recordBuf.limit(recordBuf.capacity())
          value = Some(StandardCharsets.UTF_8.decode(recordBuf).toString())
        }
      }

      sizeBuf.clear()
    }
    return value
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
