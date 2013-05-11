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

class DiskListBuilder[A](pagesMapped: Int = 4096)(implicit ser: Serializer[A]) extends Builder[A, DiskList[A]] {
  private val objSize = ser.size.toLong
  private var file: File = _
  private var raf: RandomAccessFile = _
  private var channel: FileChannel = _
  private var written: Int  = _
  private var currentMap: MappedByteBuffer = _

  initialize()

  def +=(elem: A) = synchronized {
    if (file == null) initialize()
    ser.serialize(elem, currentMap)
    written += 1
    if (currentMap.position() == currentMap.limit()) {
      currentMap = channel.map(READ_WRITE, written * ser.size, pagesMapped * objSize)
    }
    this
  }

  def result() = synchronized {
    if (file == null) initialize()
    channel.close()
    raf.close()
    val list = new DiskList(file, written, pagesMapped)
    clear()
    list
  }

  def clear() = synchronized {
    file = null
    raf = null
    channel = null
    written = 0
    currentMap = null
  }

  def initialize() = synchronized {
    file = File.createTempFile("data", ".dat")
    file.deleteOnExit()
    raf = new RandomAccessFile(file, "rw")
    channel = raf.getChannel()
    written = 0
    currentMap = channel.map(READ_WRITE, 0L, pagesMapped * objSize)
  }
}

class DiskList[A] private[util]
		(file: File, size: Int, pagesMapped: Int = 4096)(implicit ser: Serializer[A]) extends IndexedSeq[A] with IndexedSeqLike[A, DiskList[A]] {
  private val objSize = ser.size.toLong
  private val raf = new RandomAccessFile(file, "r")
  private val channel = raf.getChannel()
  private var position = 0
  private var currentMap: MappedByteBuffer = _

  map(0)

  private def map(idx: Int) = synchronized {
    val length = pagesMapped min (size - idx)
    position = idx
    currentMap = channel.map(READ_ONLY, objSize * idx, objSize * length)
  }

  override def apply(idx: Int) = synchronized {
    if (idx < 0 || idx >= size) throw new NoSuchElementException(s"Requested element $idx of $size")

    if (idx < position || idx >= position + pagesMapped) {
      map(idx)
    }
    currentMap.position(ser.size * (idx - position))
    ser.deserialize(currentMap)
  }
  override def length = size
  override def newBuilder = new DiskListBuilder

  protected override def finalize = synchronized {
    channel.close()
    raf.close()
    file.delete()
  }
}