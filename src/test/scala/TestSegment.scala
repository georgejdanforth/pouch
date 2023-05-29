package pouch

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, StandardOpenOption}
import java.util.Arrays
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll,BeforeAndAfterEach}
import scala.util.Using

class SegmentTests extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  val segmentFilePath = Files.createTempFile("test", "segment")

  override protected def afterEach(): Unit = {
    val chan = FileChannel.open(segmentFilePath, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
    chan.truncate(0)
    chan.close()
  }

  override protected def afterAll(): Unit = {
    Files.deleteIfExists(segmentFilePath)
  }

  def composeSegment(records: Array[Record]): Unit = {
    val bufSize = records.map(Encoder.getRecordSize).reduce((a, b) => a + b)
    val buffer = ByteBuffer.allocate(bufSize)
    records.foreach { record =>
      buffer.put(Encoder.encodeRecord(record))
    }

    val chan = FileChannel.open(segmentFilePath, StandardOpenOption.WRITE)
    chan.write(buffer)
    chan.close()
  }

  def b(s: String): Array[Byte] = s.getBytes

  test("segment iteration") {
    val records: Array[Record] = Array(
      Record(RecordType.Data, b("foo"), Some(b("bar"))),
      Record(RecordType.Data, b("baz"), Some(b("qux"))),
      Record(RecordType.Tombstone, b("wat"), None),
    )
    composeSegment(records)
    Using(new Segment(segmentFilePath)) { segment =>
      segment.zip(records).foreach {
        case (segRec, arrRec) => {
          assert(segRec.recordType == arrRec.recordType)
          assert(Arrays.equals(segRec.key, arrRec.key))
          segRec.recordType match {
            case RecordType.Data => assert(Arrays.equals(segRec.value.get, arrRec.value.get))
            case RecordType.Tombstone => {
              assert(segRec.value == arrRec.value)
              assert(segRec.value == None)
            }
          }
        }
      }
    }
  }
}
