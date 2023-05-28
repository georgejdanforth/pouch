package pouch

object Utils {
  private val maxIntDigits = Int.MaxValue.toString.length

  def intToSegmentNumber(i: Int): String = f"%%0${maxIntDigits}d".format(i)
  def segmentNumberToInt(s: String): Int = Integer.parseInt(s)
}
