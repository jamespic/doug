package uk.me.jamespic.dougng.util

import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode
import scala.collection.mutable.{ArrayBuffer, Map => MMap}

object MappedAllocator {
  private val ZeroCount = 4096
  private val DefaultBlockSize = 16 * 1024 * 1024
}

class MappedAllocator(blockSize: Int = MappedAllocator.DefaultBlockSize) extends Allocator {self =>
  type HandleType = (Int, Int)
  val handleSerializer = implicitly[Serializer[HandleType]]
  val nullHandle = (-1, -1)
  private val file = new FileHolder
  private val freeList = MMap.empty[Int, List[HandleType]]
  private val extents = ArrayBuffer.empty[Extent]

  def apply(size: Int) = self.synchronized {
    require(size <= blockSize, s"Cannot allocate blocks larger than $blockSize")
    val handle = if (freeList contains size) {
      val s :: tail = freeList(size)
      tail match {
        case Nil => freeList -= size
        case t => freeList += size -> t
      }
      s
    } else {
      val extent = extents.find(_.hasSpare(size)) getOrElse newExtent
      (extent.number, extent.allocate(size))
    }
    new MappedStorage(handle, size).zero
  }

  private def newExtent = {
    val number = extents.size
    val extent = new Extent(number)
    extents += extent
    extent
  }

  /*
   * Note: This method is not memory safe. If handle and size do not correspond to
   * a valid block of disk storage, undefined behaviour may occur.
   */

  def storage(handle: (Int, Int), size: Int) = self.synchronized {
    new MappedStorage(handle, size)
  }

  def close = self.synchronized {
    file.close
    freeList.clear
  }

  override def toString = self.synchronized {
    if (file.channel != null) {
      s"<MappedAllocator: File: ${file.file}, $fragmentation% fragmented>"
    } else {
      "<Closed MappedAllocator>"
    }
  }

  private def limit = if (extents.isEmpty) {
    0
  } else {
    blockSize * (extents.size - 1) + extents.last.allocated
  }

  def fragmentation = self.synchronized {
    val freeSize = (freeList.map {case (size, list) => size.toLong * list.size.toLong}).sum
    val wasteSize = (extents.dropRight(1).map {e => blockSize - e.allocated}).sum
    100.0f * (freeSize + wasteSize) / limit
  }

  private class Extent(val number: Int) {
    var allocated = 0
    val map = file.channel.map(MapMode.READ_WRITE, number * blockSize, blockSize)
    def hasSpare(size: Int) = allocated + size < blockSize
    def allocate(size: Int) = {
      require (allocated + size < blockSize, s"Cannot allocate $size bytes")
        try allocated
        finally allocated += size
    }
  }

  class MappedStorage(h: (Int, Int), size: Int) extends Storage {
    private val extent = extents(h._1)
    private val start = h._2
    override def handle = self.synchronized {
      h
    }

    private def checkBounds(off: Int, sz: Int) =
      require(off >= 0 && sz >= 0 && off + sz <= size,
          s"Offset $off, size $sz out of bounds - storage size is $size")

    private[MappedAllocator] def zero() = {
      import MappedAllocator.ZeroCount
      var remaining = size
      val zeroes = ByteBuffer.allocate(ZeroCount)
      val map = extent.map
      map.limit(start + size)
      map.position(start)
      while (remaining > 0) {
        val toDo = remaining min ZeroCount
        zeroes.limit(toDo)
        zeroes.position(0)
        map.put(zeroes)
        remaining -= toDo
      }
      this
    }

    def write[A](off: Int, value: A)(implicit ser: Serializer[A]): Unit = self.synchronized {
      checkBounds(off, ser.size)
      val map = extent.map
      map.limit(start + off + ser.size)
      map.position(start + off)
      ser.serialize(value, map)
    }

    def read[A](off: Int)(implicit ser: Serializer[A]): A = self.synchronized {
      checkBounds(off, ser.size)
      val map = extent.map
      map.limit(start + off + ser.size)
      map.position(start + off)
      ser.deserialize(map)
    }

    def free: Unit = self.synchronized {
      if (size > 0) {
        freeList += size -> (h :: freeList.getOrElse(size, Nil))
      }
    }

    override def toString = self.synchronized {
      s"${MappedAllocator.this}.MappedStorage($h, $size)"
    }
  }
}