package pouch

import java.nio.ByteBuffer
import scala.collection.mutable.TreeMap
import scala.util.Either

final case class MemTableTombstone()
final case class MemTableValue(value: Array[Byte])

class MemTable {
  implicit val ordering = new Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = {
      if (a.length == 0) {
        if (b.length == 0) {
          return 0
        }
        return -1
      }
      if (b.length == 0) {
        return 1
      }
      val len = Math.min(a.length, b.length)
      for (i <- 0 until len) {
        if (a(i) > b(i)) return 1
        if (a(i) < b(i)) return -1
      }
      if (a.length > b.length) return 1
      if (a.length < b.length) return -1
      return 0
    }
  }

  private var map = new TreeMap[Array[Byte], Either[MemTableTombstone, MemTableValue]]()
  private var size = 0

  def getSize() = size

  def get(key: Array[Byte]): Option[MemTableValue] = {
    map.get(key) match {
      case Some(Right(value)) => Some(value)
      case _ => None
    }
  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    val recordSize = Encoder.getRecordSize(key, Some(value))
    map.put(key, Right(MemTableValue(value))) match {
      case Some(Right(oldValue)) => size += recordSize - Encoder.getRecordSize(key, Some(oldValue.value))
      case Some(Left(oldValue)) => size += recordSize - Encoder.getRecordSize(key)
      case None => size += recordSize
    }
  }

  def delete(key: Array[Byte]): Unit = {
    val recordSize = Encoder.getRecordSize(key)
    map.put(key, Left(MemTableTombstone())) match {
      case Some(Right(oldValue)) => size += recordSize - Encoder.getRecordSize(key, Some(oldValue.value))
      case Some(Left(oldValue)) =>
      case None => size += recordSize
    }
  }

  def flush(): Array[Byte] = {
    val buffer = ByteBuffer.allocate(size)
    map.foreach {
      case (key, Right(value)) => buffer.put(Encoder.encodeRecord(RecordType.Data, key, Some(value.value)))
      case (key, Left(value)) => buffer.put(Encoder.encodeRecord(RecordType.Tombstone, key, None))
    }
    map = new TreeMap[Array[Byte], Either[MemTableTombstone, MemTableValue]]()
    size = 0
    return buffer.array()
  }
}
