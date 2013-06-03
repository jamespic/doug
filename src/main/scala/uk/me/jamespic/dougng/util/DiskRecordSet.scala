package uk.me.jamespic.dougng.util

import shapeless.Iso
import java.nio.channels.FileChannel.MapMode._

object DiskRecordSet {
  val DefaultExtentSize = 32
}

class DiskRecordSet[A](extentSize: Int = DiskRecordSet.DefaultExtentSize)
    (implicit ser: Serializer[A]) extends AbstractDiskRecordSet[A](extentSize) {
  private val windowSize = extentSize * ser.size
  private val file = new FileHolder(windowSize)

  private def seek(idx: Long, size: Int) = {
    if (file.hibernating) {
      file.unhibernate
      map(idx)
    }

    if (idx < position || idx > position + windowSize - size) {
      map(idx)
    }

    file.currentMap.position((idx - position).intValue)
  }

  protected def read(idx: Long) = {
    seek(idx, ser.size)
    ser.deserialize(file.currentMap)
  }

  protected def write(idx: Long, value: A) = {
    seek(idx, ser.size)
    ser.serialize(value, file.currentMap)
  }

  private def map(idx: Long) = {
    file.currentMap = file.channel.map(READ_WRITE, idx, windowSize)
    position = idx
  }

  override def toString = s"DiskRecordSet(${file.file})"
  def hibernate() = file.hibernate()
  override def sync[B](f: => B) = file.synchronized(f)
}