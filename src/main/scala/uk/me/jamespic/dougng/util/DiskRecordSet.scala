package uk.me.jamespic.dougng.util

import shapeless.Iso
import java.nio.channels.FileChannel.MapMode._

object DiskRecordSet {
  val DefaultExtentSize = 24
}

class DiskRecordSet[A](extentSize: Int = DiskRecordSet.DefaultExtentSize)
    (implicit ser: Serializer[A]) extends RecordSet[A, Long] {
  type ExtentType = DiskExtent
  type RecordType = ActiveRecord

  private val windowSize = extentSize * ser.size
  private val file = new FileHolder(windowSize)
  private var limit = 0L
  private var position = 0L

  def apply() = new DiskExtent
  private def seek(idx: Long, size: Int) = file.synchronized {
    if (file.hibernating) {
      file.unhibernate
      map(idx)
    }

    if (idx < position || idx > position + windowSize - size) {
      map(idx)
    }

    file.currentMap.position((idx - position).intValue)
  }

  def apply(index: Long) = file.synchronized {
    seek(index, ser.size)
    new ActiveRecord(index, ser.deserialize(file.currentMap))
  }

  private def map(idx: Long) = file.synchronized {
    file.currentMap = file.channel.map(READ_WRITE, idx, windowSize)
    position = idx
  }

  private def newExtent(length: Long) = file.synchronized {
    val start = limit
    limit += length
    start
  }

  override def toString = s"DiskRecordSetFactory(${file.file})"
  def hibernate() = file.hibernate()
  override def sync[B](f: => B) = file.synchronized(f)


  class ActiveRecord private[DiskRecordSet](val index: Long, private var a: A) extends Record {
    def apply() = a
    def update(v: A) = a = v
    def save = file.synchronized {
      seek(index, ser.size)
      ser.serialize(a, file.currentMap)
    }
    override def toString = s"ActiveRecord($index, $a)"
  }


  class DiskExtent extends Extent {
    private var extentLength = ser.size * extentSize
    private var extentStart = newExtent(extentLength)

    def apply(a: A) = file.synchronized {
      require(extentLength >= 0)
      if (extentLength == 0) {
        extentLength = extentSize * ser.size
        extentStart = newExtent(extentLength)
      }
      seek(extentStart, ser.size)
      val loc = extentStart
      ser.serialize(a, file.currentMap)
      extentStart += ser.size
      extentLength -= ser.size
      new ActiveRecord(loc, a)
    }

    override def toString = s"DiskRecordSet(${file.file}, $ser)"
  }
}