package pouch

import java.util.Arrays
import org.scalatest.funsuite.AnyFunSuite

class EncoderTests extends AnyFunSuite {

  test("getRecordSize with value") {
    val key = "foo".getBytes
    val value = "bar".getBytes
    val expected = java.lang.Integer.BYTES * 2 + java.lang.Character.BYTES + key.length + value.length
    val result = Encoder.getRecordSize(key, Some(value))
    assert(result == expected)
  }

  test("getRecordSize without value") {
    val key = "foo".getBytes
    val expected = java.lang.Integer.BYTES * 2 + java.lang.Character.BYTES + key.length
    val result = Encoder.getRecordSize(key)
    assert(result == expected)
  }

  test("encodeRecord with data record") {
    val key = "foo".getBytes
    val value = "bar".getBytes
    val expected = Array[Byte](
      0x00, 0x00, 0x00, 0x10, // record length
      0x00, 0x00, 0x00, 0x03,  // key length
      0x00, 0x44, // record type
      0x66, 0x6f, 0x6f, // key
      0x62, 0x61, 0x72, // value
    )
    val result = Encoder.encodeRecord(RecordType.Data, key, Some(value))
    assert(Arrays.equals(result, expected))
  }

  test("encodeRecord with tombstone record") {
    val key = "foo".getBytes
    val expected = Array[Byte](
      0x00, 0x00, 0x00, 0x0d, // record length
      0x00, 0x00, 0x00, 0x03,  // key length
      0x00, 0x54, // record type
      0x66, 0x6f, 0x6f, // key
    )
    val result = Encoder.encodeRecord(RecordType.Tombstone, key, None)
    assert(Arrays.equals(result, expected))
  }
}
