package uk.me.jamespic.dougng.util

import scala.collection.mutable.{Map => MMap}
import java.nio.ByteBuffer

object PageCacheAllocator {
}

class PageCacheAllocator(cacheSize: Int = 2048) extends AbstractFileAllocator {self =>
  require((cacheSize & (cacheSize - 1)) == 0, s"Cache size $cacheSize is not a power of 2" )
  private[this] val cacheMask = cacheSize - 1
  private val pageCache = new Array[Page](cacheSize)

  private def getPage(handle: Long, size: Int) = {
    val address = hash(handle)
    var page = pageCache(address)
    if (page != null && page.handle != handle) {
      page.unload
      page = null
    }
    if (page == null) {
      page = new Page(handle, size)
      pageCache(address) = page
    }
    page
  }

  @inline private def hash(handle: Long) = {
    (handle ^ (handle >> 2)).toInt & cacheMask
  }

  def apply(size: Int) = self.synchronized {
    val start = allocate(size)
    getPage(start, size).zero
    storage(start, size)
  }

  def storage(handle: Long, size: Int): PageCacheStorage = self.synchronized {
    new PageCacheStorage(handle, size)
  }

  class PageCacheStorage private[PageCacheAllocator](val handle: Long, size: Int) extends Storage {
    private[this] var valid = true

    private def findPage = {
      getPage(handle, size)
    }

    def free = self.synchronized {
      checkValid
      valid = false
      findPage.free
      doFree(handle, size)
    }

    def read[A](off: Int)(implicit ser: Serializer[A]) = self.synchronized {
      checkValid
      val buffer = findPage.buffer
      buffer.limit(off + ser.size)
      buffer.position(off)
      ser.deserialize(buffer)
    }

    def write[A](off: Int,value: A)(implicit ser: Serializer[A]) = self.synchronized{
      checkValid
      val page = findPage
      val buffer = page.buffer
      val size = ser.size
      buffer.limit(off + size)
      buffer.position(off)
      ser.serialize(value, buffer)
      page.dirtyStart = page.dirtyStart min off
      page.dirtyEnd = page.dirtyEnd max off + size
    }

    private def checkValid = require(valid, "Use after free")
  }

  private[PageCacheAllocator] class Page(val handle: Long, size: Int) {
    def dirty = dirtyStart <= dirtyEnd
    /*
     * initialise dirtyStart to size and dirtyEnd to 0, so
     * that max and min will bring them into line when dirtied
     */
    var dirtyStart = size
    var dirtyEnd = 0
    private[this] var _buffer: ByteBuffer = null

    def markClean = {
      dirtyStart = size
      dirtyEnd = 0
    }

    def buffer = {
      if (_buffer == null) {
        _buffer = ByteBuffer.allocate(size)
        file.channel.read(_buffer, handle)
        markClean
      }
      _buffer
    }

    def zero = {
      _buffer = ByteBuffer.allocate(size)
      dirtyStart = 0
      dirtyEnd = size
      this
    }

    def unload = {
      if (dirty) {
        _buffer.position(dirtyStart)
        _buffer.limit(dirtyEnd)
        file.channel.write(buffer, handle + dirtyStart)
      }
      markClean
      _buffer = null
    }

    def free = {
      pageCache(hash(handle)) = null
    }
  }
}