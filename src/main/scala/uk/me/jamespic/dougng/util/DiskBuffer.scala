package uk.me.jamespic.dougng.util

import scala.collection.mutable.Builder
import scala.collection.IndexedSeqLike
import java.io.{RandomAccessFile, File}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import FileChannel.MapMode._
import scala.collection.generic.CanBuildFrom

object DiskBuffer {
  implicit def canBuildFrom[B: Serializer] = new CanBuildFrom[DiskBuffer[_], B, DiskBuffer[B]] {
    override def apply() = new DiskBuffer
    override def apply(dl: DiskBuffer[_]) = new DiskBuffer
  }

  def apply[A: Serializer](l: A*) = l.to[DiskBuffer]
}

class DiskBuffer[A](pagesMapped: Int = 256)(implicit ser: Serializer[A])
    extends IndexedSeq[A]
    with IndexedSeqLike[A, DiskBuffer[A]]
    with Builder[A, DiskBuffer[A]]
    with Hibernatable
    /* with RecordSet[A] */ {
  type IndexType = Int
  type RecordType = ActiveRecord
  private val objSize = ser.size.toLong
  private val file = new FileHolder(pagesMapped * objSize)
  private var len  = 0
  private var position = 0

  def +=(elem: A) = file.synchronized {
    update(len, elem)
    this
  }

  def result() = this

  def clear() = file.synchronized {
    len = 0
    map(0)
  }

  private def map(idx: Int) = file.synchronized {
    file.currentMap = file.channel.map(READ_WRITE, objSize * idx, objSize * pagesMapped)
  }

  private def seek(idx: Int) = file.synchronized {
    if (file.hibernating) {
      file.unhibernate
      map(idx)
    }

    if (idx < position || idx >= position + pagesMapped) {
      map(idx)
    }

    file.currentMap.position(ser.size * (idx - position))
  }

  override def apply(idx: Int) = file.synchronized {
    if (idx < 0 || idx >= len) throw new NoSuchElementException(s"Requested element $idx of $len")
    seek(idx)
    ser.deserialize(file.currentMap)
  }

  def update(idx: Int, v: A) = file.synchronized {
    if (idx < 0 || idx > len) throw new NoSuchElementException(s"Requested element $idx of $len")
    seek(idx)
    ser.serialize(v, file.currentMap)
    if (idx == len) len += 1
  }

  override def length = file.synchronized(len)
  override def newBuilder = new DiskBuffer
  override def toString = file.synchronized(s"DiskBuffer(${file.file})")

  /**
   * Advise this DiskBuffer that it may not be needed for a while, and can close files
   */
  def hibernate() = file.hibernate()
  def close() = file.close()
  override def sync[B](f: => B) = file.sync(f)

  def addRec(v: A) = file.synchronized {
    this += v
    new ActiveRecord(len - 1, v)
  }

  def getRec(idx: Int) = file.synchronized {
    new ActiveRecord(idx, apply(idx))
  }

  class ActiveRecord private[DiskBuffer](val index: Int, private var value: A) /* extends Record */ {
    def apply() = value
    def update(v: A) = value = v
    def save = DiskBuffer.this(index) = value
    override def toString = s"ActiveRecord($value)"
  }

  object ActiveRecord {
    implicit def record2Value(rec: ActiveRecord) = rec()
  }

}