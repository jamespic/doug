package uk.me.jamespic.dougng.util

import shapeless.Iso
import java.nio.channels.FileChannel.MapMode._

object AbstractDiskRecordSet {
  val DefaultExtentSize = 32
}

abstract class AbstractDiskRecordSet[A](extentSize: Int = AbstractDiskRecordSet.DefaultExtentSize)
    (implicit ser: Serializer[A]) extends RecordSet[A, Long] {
  type ExtentType = DiskExtent
  type RecordType = ActiveRecord

  protected var limit = 0L
  protected var position = 0L

  def apply() = sync {new DiskExtent}
  def apply(index: Long) = sync {
    new ActiveRecord(index, read(index))
  }

  protected def read(idx: Long): A

  protected def write(idx: Long, value: A): Unit

  protected def newExtent(length: Long) = {
    val start = limit
    limit += length
    start
  }

  class ActiveRecord private[AbstractDiskRecordSet](val index: Long, private var a: A) extends Record {
    def apply() = a
    def update(v: A) = a = v
    def save = sync {
      write(index, a)
    }
    override def toString = s"ActiveRecord($index, $a)"
  }


  class DiskExtent private[AbstractDiskRecordSet] extends Extent {
    private var extentLength = ser.size * extentSize
    private var extentStart = newExtent(extentLength)

    def apply(a: A) = sync {
      require(extentLength >= 0)
      if (extentLength == 0) {
        extentLength = extentSize * ser.size
        extentStart = newExtent(extentLength)
      }
      val loc = extentStart
      write(loc, a)
      extentStart += ser.size
      extentLength -= ser.size
      new ActiveRecord(loc, a)
    }

    override def toString = s"DiskExtent(${AbstractDiskRecordSet.this}, $ser)"
  }
}