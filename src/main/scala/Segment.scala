package pouch

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files,Path}

class Segment(path: Path) extends Iterable[Record] with AutoCloseable {
  private val chan = new RandomAccessFile(path.toString(), "r").getChannel
  private val sizeBuffer = ByteBuffer.allocate(java.lang.Integer.BYTES)

  def iterator: Iterator[Record] = {
    new Iterator[Record] {

      def hasNext: Boolean = chan.position < chan.size

      def next(): Record = {
        if (!hasNext) {
          throw new NoSuchElementException("No more records in segment")
        }
        chan.read(sizeBuffer)
        sizeBuffer.flip()
        chan.position(chan.position - java.lang.Integer.BYTES)

        val recordSize = sizeBuffer.getInt()
        sizeBuffer.clear()

        val recordBuffer = ByteBuffer.allocate(recordSize)
        chan.read(recordBuffer)
        recordBuffer.flip()

        return Encoder.decodeRecord(recordBuffer)
      }
    }
  }

  def close(): Unit = {
    chan.close()
  }
}
