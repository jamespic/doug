package uk.me.jamespic.dougng.util

trait AbstractFileAllocator extends Allocator {self =>
  type HandleType = Long
  val handleSerializer = Serializer.LongSerializer
  val nullHandle = -1L
  protected[this] val file = new FileHolder()
  protected[this] var freeList = Map.empty[Int, List[Long]]
  protected[this] var limit = 0L

  protected def allocate(size: Int) = {
    if (freeList contains size) {
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
  }

  def close = self.synchronized {
    file.close
    freeList = Map.empty
  }

  override def toString = self.synchronized {
    if (file.channel != null) {
      s"<${getClass.getSimpleName()}: File: ${file.file}, $fragmentation% fragmented>"
    } else {
      s"<Closed ${getClass.getSimpleName()}>"
    }
  }

  def fragmentation = self.synchronized {
    val freeSize = (freeList.map {case (size, list) => size.toLong * list.size.toLong}).sum
    100.0f * freeSize / limit
  }

  protected[this] def doFree(handle: Long, size: Int) = {
    if (size > 0) {
      freeList += size -> (handle :: freeList.getOrElse(size, Nil))
    }
  }
}