package uk.me.jamespic.dougng.util

import java.nio.ByteBuffer

object ChannelAllocator {
  private val ZeroCount = 4096
  private val InternalBufferSize = 8192
}

class ChannelAllocator extends AbstractFileAllocator {self =>
  private val buf = ByteBuffer.allocate(ChannelAllocator.InternalBufferSize)

  def apply(size: Int) = self.synchronized {
    val start = allocate(size)
    new ChannelStorage(start, size).zero
  }

  /*
   * Note: This method is not memory safe. If handle and size do not correspond to
   * a valid block of disk storage, undefined behaviour may occur.
   */

  def storage(handle: Long, size: Int) = self.synchronized {
    new ChannelStorage(handle, size)
  }

  class ChannelStorage(start: Long, size: Int) extends Storage {
    private var valid = true
    def handle = self.synchronized {
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

    def write[A](off: Int, value: A)(implicit ser: Serializer[A]): Unit = self.synchronized {
      checkValid
      checkBounds(off, ser.size)
      val buff = buffer(ser.size)
      ser.serialize(value, buff)
      buff.rewind()
      file.channel.write(buff, start + off)
    }

    def read[A](off: Int)(implicit ser: Serializer[A]): A = self.synchronized {
      checkValid
      checkBounds(off, ser.size)
      val buf = buffer(ser.size)
      file.channel.read(buf, start + off)
      buf.rewind()
      ser.deserialize(buf)
    }

    def free: Unit = self.synchronized {
      checkValid
      valid = false
      doFree(start, size)
    }

    override def toString = self.synchronized {
      if (valid) {
        s"${ChannelAllocator.this}.ChannelStorage($start, $size)"
      } else {
        "<Invalid Channel Allocator>"
      }
    }
  }
}