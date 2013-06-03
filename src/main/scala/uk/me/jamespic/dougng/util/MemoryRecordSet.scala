package uk.me.jamespic.dougng.util

import scala.collection.mutable.ArrayBuffer

class MemoryRecordSet[A] extends RecordSet[A, Int] {
  type ExtentType = MemoryExtent
  type RecordType = MemoryRecord
  private val records = ArrayBuffer.empty[A]

  def hibernate() = {}
  def apply() = new MemoryExtent
  def apply(i: Int) = sync {new MemoryRecord(records(i), i)}

  class MemoryExtent extends Extent {
    def apply(a: A) = sync {
      records += a
      new MemoryRecord(a, records.size - 1)
    }
  }

  class MemoryRecord private[MemoryRecordSet](private var a: A, val index: Int) extends Record {
    def apply() = a
    def update(o: A) = a = o
    def save = sync {records(index) = a}
  }
}