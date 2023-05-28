package pouch

import org.scalatest.funsuite.AnyFunSuite

class UtilsTests extends AnyFunSuite {
  test("intToSegmentNumber") {
    assert(Utils.intToSegmentNumber(0) == "0000000000")
    assert(Utils.intToSegmentNumber(1) == "0000000001")
    assert(Utils.intToSegmentNumber(10) == "0000000010")
    assert(Utils.intToSegmentNumber(100) == "0000000100")
    assert(Utils.intToSegmentNumber(1000) == "0000001000")
    assert(Utils.intToSegmentNumber(Int.MaxValue) == "2147483647")
  }

  test("segmentNumberToInt") {
    assert(Utils.segmentNumberToInt("0000000000") == 0)
    assert(Utils.segmentNumberToInt("0000000001") == 1)
    assert(Utils.segmentNumberToInt("0000000010") == 10)
    assert(Utils.segmentNumberToInt("0000000100") == 100)
    assert(Utils.segmentNumberToInt("0000001000") == 1000)
    assert(Utils.segmentNumberToInt("2147483647") == Int.MaxValue)
  }
}
