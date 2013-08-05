package uk.me.jamespic.dougng.util

import scala.collection.mutable.ArrayBuffer
import java.nio.ByteBuffer

class DebuggingAllocator extends AbstractMemoryAllocator {self =>
  type HandleType = Int
  val storage = ArrayBuffer.empty[DebuggingStorage]
  def apply(size: Int) = self.synchronized {
    val res = new DebuggingStorage(size, storage.size)
    storage += res
    res
  }
  implicit lazy val handleSerializer = Serializer.IntSerializer
  val nullHandle = -1
  def storage(handle: HandleType, size: Int) = self.synchronized {
    val s = storage(handle)
    require(s.size == size)
    s
  }

  class DebuggingStorage protected[DebuggingAllocator](val size: Int, val handle: Int)
      extends AbstractMemoryStorage(size) {
    private var valid = true
    override def free: Unit = {valid = false}
    override def read[A](off: Int)(implicit ser: Serializer[A]) = self.synchronized {
      require(valid)
      super.read(off)(ser)
    }
    override def write[A](off: Int,value: A)(implicit ser: Serializer[A]) = self.synchronized {
      require(valid)
      super.write(off, value)(ser)
    }
  }
}