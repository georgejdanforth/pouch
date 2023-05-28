package pouch

import java.util.Arrays
import org.scalatest.funsuite.AnyFunSuite

class MemTableTests extends AnyFunSuite {
  test("put get") {
    val memTable = new MemTable()
    val key = "foo".getBytes
    val value = "bar".getBytes
    val expectedSize = Encoder.getRecordSize(key, Some(value))
    memTable.put(key, value)
    assert(memTable.getSize() == expectedSize)
    assert(Arrays.equals(memTable.get(key).get.value, value))
  }

  test("put delete get") {
    val memTable = new MemTable()
    val key = "foo".getBytes
    val value = "bar".getBytes
    val expectedSize = Encoder.getRecordSize(key)
    memTable.put(key, value)
    memTable.delete(key)
    assert(memTable.getSize() == expectedSize)
    assert(memTable.get(key) == None)
  }

  test("put put get same key") {
    val memTable = new MemTable()
    val key = "foo".getBytes
    val value = "bar".getBytes
    val expectedSize = Encoder.getRecordSize(key, Some(value))
    memTable.put(key, value)
    memTable.put(key, value)
    assert(memTable.getSize() == expectedSize)
    assert(Arrays.equals(memTable.get(key).get.value, value))
  }

  test("put put get different keys") {
    val memTable = new MemTable()
    val k1 = "foo".getBytes
    val v1 = "bar".getBytes
    val k2 = "baz".getBytes
    val v2 = "qux".getBytes
    val expectedSize = Encoder.getRecordSize(k1, Some(v1)) + Encoder.getRecordSize(k2, Some(v2))
    memTable.put(k1, v1)
    memTable.put(k2, v2)
    assert(memTable.getSize() == expectedSize)
    assert(Arrays.equals(memTable.get(k1).get.value, v1))
    assert(Arrays.equals(memTable.get(k2).get.value, v2))
  }

  test("put put get same key different value") {
    val memTable = new MemTable()
    val key = "foo".getBytes
    val value1 = "bar".getBytes
    val value2 = "barr".getBytes
    val expectedSize = Encoder.getRecordSize(key, Some(value2))
    memTable.put(key, value1)
    memTable.put(key, value2)
    assert(memTable.getSize() == expectedSize)
    assert(Arrays.equals(memTable.get(key).get.value, value2))
  }

  test("put delete missing key") {
    val memTable = new MemTable()
    val key = "foo".getBytes
    val value = "bar".getBytes
    val delKey = "baz".getBytes
    val expectedSize = Encoder.getRecordSize(key, Some(value)) + Encoder.getRecordSize(delKey)
    memTable.put(key, value)
    memTable.delete(delKey)
    assert(memTable.getSize() == expectedSize)
    assert(Arrays.equals(memTable.get(key).get.value, value))
    assert(memTable.get(delKey) == None)
  }

  test("flush ordering") {
    val memTable = new MemTable()
    val (k1, v1, k2, v2) = ("foo".getBytes, "bar".getBytes, "baz".getBytes, "qux".getBytes)
    val expected =
      Encoder.encodeRecord(RecordType.Data, k2, Some(v2)) ++
      Encoder.encodeRecord(RecordType.Data, k1, Some(v1))
    memTable.put(k1, v1)
    memTable.put(k2, v2)
    val result = memTable.flush()
    assert(memTable.getSize() == 0)
    assert(memTable.get("foo".getBytes) == None)
    assert(memTable.get("baz".getBytes) == None)
    assert(Arrays.equals(result, expected))
  }

  test("flush with delete") {
    val memTable = new MemTable()
    val (k1, v1, k2, v2) = ("foo".getBytes, "bar".getBytes, "baz".getBytes, "qux".getBytes)
    val expected =
      Encoder.encodeRecord(RecordType.Data, k2, Some(v2)) ++
      Encoder.encodeRecord(RecordType.Tombstone, k1, None)
    memTable.put(k1, v1)
    memTable.put(k2, v2)
    memTable.delete(k1)
    val result = memTable.flush()
    assert(memTable.getSize() == 0)
    assert(memTable.get("foo".getBytes) == None)
    assert(memTable.get("baz".getBytes) == None)
    assert(Arrays.equals(result, expected))
  }
}
