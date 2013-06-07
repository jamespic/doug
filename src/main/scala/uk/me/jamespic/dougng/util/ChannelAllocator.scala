package uk.me.jamespic.dougng.util

import java.nio.ByteBuffer

object ChannelAllocator {
  private val ZeroCount = 4096
  private val InternalBufferSize = 8192
}

class ChannelAllocator extends Allocator {
  type HandleType = Long
  val handleSerializer = implicitly[Serializer[HandleType]]
  val nullHandle = -1L
  private val file = new FileHolder(0)
  private var freeList = Map.empty[Int, List[Long]]
  private var limit = 0L
  private val buf = ByteBuffer.allocate(ChannelAllocator.InternalBufferSize)
  override def sync[B](f: => B) = file.sync(f)
  def hibernate() = file.hibernate()

  def apply(size: Int) = sync {
    val start = if (freeList contains size) {
      val s :: tail = freeList(size)
      tail match {
        case Nil => freeList -= size
        case t => freeList += size -> t
      }
      s
    } else {
      val s = limit
      limit += size
      s
    }
    new ChannelStorage(start, size).zero
  }

  /*
   * Note: This method is not memory safe. If handle and size do not correspond to
   * a valid block of disk storage, undefined behaviour may occur.
   */

  def storage(handle: Long, size: Int) = sync {
    new ChannelStorage(handle, size)
  }

  def close = sync {
    file.close
    freeList = Map.empty
  }

  override def toString = sync {
    if (file.channel != null) {
      s"<ChannelAllocator: File: ${file.file}, $fragmentation% fragmented>"
    } else {
      "<Closed ChannelAllocator>"
    }
  }

  def fragmentation = sync {
    val freeSize = (freeList.map {case (size, list) => size.toLong * list.size.toLong}).sum
    100.0f * freeSize / limit
  }

  class ChannelStorage(start: Long, size: Int) extends Storage {
    private var valid = true
    def handle = sync {
      checkValid
      start
    }

    private def checkValid = require(valid, "Use after free")
    private def checkBounds(off: Int, sz: Int) =
      require(off >= 0 && sz >= 0 && off + sz <= size,
          s"Offset $off, size $sz out of bounds - storage size is $size")

    private[ChannelAllocator] def zero() = {
      import ChannelAllocator.ZeroCount
      var remaining = size
      val zeroes = ByteBuffer.allocate(ZeroCount)
      val chan = file.channel
      if (chan.position != start) chan.position(start)
      while (remaining > 0) {
        val toDo = remaining min ZeroCount
        zeroes.limit(toDo)
        zeroes.position(0)
        chan.write(zeroes)
        remaining -= toDo
      }
      this
    }

    private def buffer(size: Int) = {
      if (size <= ChannelAllocator.InternalBufferSize) {
        buf.clear()
        buf.limit(size)
        buf
      } else {
        ByteBuffer.allocate(size)
      }
    }

    def write[A](off: Int, value: A)(implicit ser: Serializer[A]): Unit = sync {
      checkValid
      checkBounds(off, ser.size)
      val buff = buffer(ser.size)
      ser.serialize(value, buff)
      buff.rewind()
      file.channel.write(buff, start + off)
    }

    def read[A](off: Int)(implicit ser: Serializer[A]): A = sync {
      checkValid
      checkBounds(off, ser.size)
      val buf = buffer(ser.size)
      file.channel.read(buf, start + off)
      buf.rewind()
      ser.deserialize(buf)
    }

    def free: Unit = sync {
      checkValid
      valid = false
      if (size > 0) {
        freeList += size -> (start :: freeList.getOrElse(size, Nil))
      }
    }

    override def toString = sync {
      if (valid) {
        s"${ChannelAllocator.this}.ChannelStorage($start, $size)"
      } else {
        "<Invalid Channel Allocator>"
      }
    }
  }
}