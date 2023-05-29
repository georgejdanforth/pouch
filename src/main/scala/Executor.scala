package pouch

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files,Path,StandardOpenOption}
import java.util.Arrays

class Executor(baseDataDir: Path, walService: WALService) {
  private final val walDir = baseDataDir.resolve("wal")
  private final val dataDir = baseDataDir.resolve("data")

  private final val memTable = new MemTable()

  def get(key: Array[Byte]): Option[Array[Byte]] = {
    memTable.get(key) match {
      case Some(value) => Some(value.value)
      case None => None
    }
  }

  def set(key: Array[Byte], value: Array[Byte]): Unit = {
    memTable.put(key, value)
  }

  def delete(key: Array[Byte]): Unit = {
    memTable.delete(key)
  }

  def recover(): Unit = {
    // TODO: recover from current WAL segment
  }

  // TODO: This is the original implementation making use of a single unsorted segment.
  //  Currently, we're using a memtable only with no WAL. Use these methods to help
  //  implement the new methods using SSTables + MemTable + WAL.
  /*
  def get(key: Array[Byte]): Option[Array[Byte]] = {
    var value: Option[Array[Byte]] = None
    val chan = new RandomAccessFile(dataFile.toString(), "r").getChannel

    val sizeBuf = ByteBuffer.allocate(java.lang.Integer.BYTES * 2)

    while (chan.position < chan.size) {
      chan.read(sizeBuf)
      sizeBuf.flip()
      val recordBytes = sizeBuf.getInt()
      val keyBytes = sizeBuf.getInt()
      val valBytes = recordBytes - 2 * java.lang.Integer.BYTES - java.lang.Character.BYTES - keyBytes

      val recordBuf = ByteBuffer.allocate(recordBytes - 2 * java.lang.Integer.BYTES)
      chan.read(recordBuf)
      recordBuf.flip()

      val recordType = recordBuf.getChar()
      recordBuf.limit(java.lang.Character.BYTES + keyBytes)
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
    val record = Encoder.encodeRecord(RecordType.Data, key, Some(value))
    Files.write(dataFile, record, StandardOpenOption.APPEND)
  }

  def delete(key: Array[Byte]): Unit = {
    val record = Encoder.encodeRecord(RecordType.Tombstone, key, None)
    Files.write(dataFile, record, StandardOpenOption.APPEND)
  }
  */
}
