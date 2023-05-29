package pouch

import java.nio.file.{Files,Path}

class WALService(walDir: Path, currentSegmentNumber: Int) {
  private var segmentNumber = currentSegmentNumber

  def getSegmentNumber(): Int = segmentNumber
}
