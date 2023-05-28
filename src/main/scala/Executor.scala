package pouch

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files,Path,StandardOpenOption}
import java.util.Arrays

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

  private def encode(recordType: Char, key: Array[Byte], value: Array[Byte]): Array[Byte] = {
    val bufSize = 2 * intBytes + charBytes + key.length + value.length
    val buffer = ByteBuffer.allocate(bufSize)

    buffer.putInt(bufSize)
    buffer.putInt(key.length)
    buffer.putChar(recordType)
    buffer.put(key)
    buffer.put(value)

    return buffer.array()
  }

  def get(key: Array[Byte]): Option[Array[Byte]] = {
    var value: Option[Array[Byte]] = None
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
      val recordKey = Array.ofDim[Byte](keyBytes)
      recordBuf.get(recordKey)

      if (Arrays.equals(recordKey, key)) {
        if (recordType == RecordType.Tombstone) {
          value = None
        } else {
          recordBuf.limit(recordBuf.capacity)
          value = Some(Array.ofDim[Byte](valBytes))
          recordBuf.get(value.get)
        }
      }

      sizeBuf.clear()
    }
    return value
  }

  def set(key: Array[Byte], value: Array[Byte]): Unit = {
    val record = encode(RecordType.Data, key, value)
    Files.write(dataFile, record, StandardOpenOption.APPEND)
  }

  def delete(key: Array[Byte]): Unit = {
    val record = encode(RecordType.Tombstone, key, Array[Byte]())
    Files.write(dataFile, record, StandardOpenOption.APPEND)
  }
}
