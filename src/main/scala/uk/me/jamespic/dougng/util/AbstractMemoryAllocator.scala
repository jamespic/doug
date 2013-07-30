package uk.me.jamespic.dougng.util

import java.nio.ByteBuffer

abstract class AbstractMemoryAllocator extends Allocator {
  def close: Unit = ()
  def hibernate(): Unit = ()

  abstract class AbstractMemoryStorage(size: Int) extends Storage {
    private val data = ByteBuffer.allocate(size)
    def free: Unit = ()
    def read[A](off: Int)(implicit ser: Serializer[A]) = sync {
      data.limit(off + ser.size)
      data.position(off)
      ser.deserialize(data)
    }
    def write[A](off: Int,value: A)(implicit ser: Serializer[A]) = sync {
      data.limit(off + ser.size)
      data.position(off)
      ser.serialize(value, data)
    }
  }
}