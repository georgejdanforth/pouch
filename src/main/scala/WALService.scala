package pouch

import java.nio.file.{Files,Path,StandardOpenOption}
import scala.util.Using

class WALService(walDir: Path, currentSegmentNumber: Int) {
  private var segmentNumber = currentSegmentNumber

  def getSegmentNumber(): Int = segmentNumber

  def getSegmentPath(n: Option[Int] = None) = {
    walDir.resolve(Utils.intToSegmentNumber({
      n match {
        case Some(n) => n
        case None => segmentNumber
      }
    }))
  }

  def getSegment(n: Option[Int] = None): Segment = {
    return new Segment(getSegmentPath(n))
  }

  def append(recordType: Char, key: Array[Byte], value: Option[Array[Byte]]): Unit = {
    Files.write(
      getSegmentPath(),
      Encoder.encodeRecord(recordType, key, value),
      StandardOpenOption.APPEND
    )
  }
}
