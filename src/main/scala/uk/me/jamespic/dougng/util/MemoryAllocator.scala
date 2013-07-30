package uk.me.jamespic.dougng.util

import java.nio.ByteBuffer

object MemoryAllocator extends AbstractMemoryAllocator {
  type HandleType = MemoryStorage
  def apply(size: Int) = new MemoryStorage(size)
  implicit lazy val handleSerializer = {
    throw new UnsupportedOperationException("Memory Allocators are not Serializable")
  }
  val nullHandle = null
  def storage(handle: HandleType, size: Int) = handle

  final class MemoryStorage private[MemoryAllocator](size: Int) extends AbstractMemoryStorage(size) {
    def handle = this
  }
}