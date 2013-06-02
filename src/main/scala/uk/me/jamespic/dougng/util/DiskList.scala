package uk.me.jamespic.dougng.util

import scala.collection.mutable.Builder
import scala.collection.IndexedSeqLike
import java.io.{RandomAccessFile, File}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import FileChannel.MapMode._
import scala.collection.generic.CanBuildFrom

object DiskList {
  implicit def canBuildFrom[B: Serializer] = new CanBuildFrom[DiskList[_], B, DiskList[B]] {
    override def apply() = new DiskListBuilder
    override def apply(dl: DiskList[_]) = new DiskListBuilder
  }

  def apply[A: Serializer](l: A*) = l.to[DiskList]
}

class DiskListBuilder[A](writePagesMapped: Int = 256, readPagesMapped: Int = 32)(implicit ser: Serializer[A]) extends Builder[A, DiskList[A]] {
  private val objSize = ser.size.toLong
  private var file = new FileHolder(writePagesMapped * objSize)
  private var written: Int  = 0

  initialize()

  def +=(elem: A) = synchronized {
    if (file == null) initialize()
    file.synchronized {
      ser.serialize(elem, file.currentMap)
      written += 1
      if (file.currentMap.position() == file.currentMap.limit()) {
        file.currentMap = file.channel.map(READ_WRITE, written * ser.size, writePagesMapped * objSize)
      }
      this
    }

  }

  def result() = synchronized {
    if (file == null) initialize()
    file.synchronized {
      val list = new DiskList(file, written, readPagesMapped)
      clear()
      list
    }
  }

  def clear() = synchronized {
    file = null
    written = 0
  }

  private def initialize() = synchronized {
    file = new FileHolder(writePagesMapped * objSize)
    written = 0
  }
}

class DiskList[A] private[util]
		(file: FileHolder, size: Int, pagesMapped: Int)(implicit ser: Serializer[A]) extends IndexedSeq[A] with IndexedSeqLike[A, DiskList[A]] {
  private val objSize = ser.size.toLong
  private var position = 0

  map(0)

  private def map(idx: Int) = file.synchronized {
    val length = pagesMapped min (size - idx)
    position = idx
    file.currentMap = file.channel.map(READ_ONLY, objSize * idx, objSize * length)
  }

  override def apply(idx: Int) = file.synchronized {
    if (idx < 0 || idx >= size) throw new NoSuchElementException(s"Requested element $idx of $size")

    if (file.hibernating) {
      file.unhibernate
      map(idx)
    }

    if (idx < position || idx >= position + pagesMapped) {
      map(idx)
    }

    file.currentMap.position(ser.size * (idx - position))
    ser.deserialize(file.currentMap)
  }
  override def length = size
  override def newBuilder = new DiskListBuilder
  override def toString = file.synchronized(s"DiskList(${file.raf})")

  /**
   * Advise this DiskList that it may not be needed for a while, and can close files
   */
  def hibernate() = file.hibernate()
  def close() = file.close()
}