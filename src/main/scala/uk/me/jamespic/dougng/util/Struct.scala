package uk.me.jamespic.dougng.util

import scala.collection.mutable.{Buffer, BufferLike}
import java.nio.ByteBuffer

class Struct[S <: Struct[S]](val storage: Allocator#Storage, private val offset: Int) {
  this: S =>
  private var _size = 0
  def size = _size
  def free = storage.free

  protected final def __var__[X](implicit ser: Serializer[X]) = {
    val newVar = new Variable[X](offset + _size)
    _size += ser.size
    newVar
  }

  protected final def __struct__[X <: Struct[X]](implicit info: StructInfo[X]): X = {
    val newStruct = info.cons(storage, offset + _size)
    _size += info.size
    newStruct
  }

  protected final def __buffer__[X](limit: Int)(implicit ser: Serializer[X]) = {
    val newSeq = new VarSequence[X](offset + _size, limit)
    _size += 4 + limit * ser.size
    newSeq
  }

  protected final def __struct_buffer__[X <: Struct[X]](limit: Int)(implicit info: StructInfo[X]) = {
    val newSeq = new StructSequence[X](offset + _size, limit)
    _size += 4 + limit * info.size
    newSeq
  }

  protected final def __array__[X <: Struct[X]](limit: Int)(implicit info: StructInfo[X]) = {
    for (i <- 0 to limit) yield __struct__[X]
  }

  def :=(o: S) = {
    require(o._size == this._size)
    val copySerializer = Serializer.byteArraySerializer(_size)
    val bytes = o.storage.read(o.offset)(copySerializer)
    storage.write(offset, bytes)(copySerializer)
  }

  def deepCopy(implicit info: StructInfo[S]) = {
    val copy = info.allocate(MemoryAllocator)
    copy := this
    copy
  }

  final class Variable[X: Serializer] private[Struct](start: Int) {
    private var cache: Option[X] = None
    def apply() = cache match {
      case None =>
        val ret = storage.read[X](start)
        cache = Some(ret)
        ret
      case Some(ret) => ret
    }
    def update(x: X) = {
      cache = Some(x)
      storage.write(start, x)
    }
    override def toString = s"Variable(${apply()})"
  }

  sealed abstract class AbstractSequence[X] private[Struct](start: Int, limit: Int, protected val itemSize: Int)
      extends Buffer[X] {
    private var _sz: Option[Int] = None
    private def size_=(newSize: Int) = {
      _sz = Some(newSize)
      storage.write(start, newSize)
    }
    def length = _sz match {
      case None =>
        val sz = storage.read[Int](start)
        _sz = Some(sz)
        sz
      case Some(sz) =>
        sz
    }

    protected val dataStart = start + 4

    private def copyRecords(from: Int, to: Int, count: Int) = {
      if (count != 0) {
        val copySerializer = Serializer.byteArraySerializer(count * itemSize)
        val arr = storage.read(dataStart + from * itemSize)(copySerializer)
        storage.write(dataStart + to * itemSize, arr)(copySerializer)
      }
    }

    def apply(idx: Int) = {
      require(idx >= 0 && idx < length)
      innerGet(idx)
    }

    protected def innerGet(idx: Int): X
    protected def innerPut(idx: Int, x: X): Unit

    /** As seen from class Sequence, the missing signatures are as follows.
     * For convenience, these are usable as stub implementations. */
    // Members declared in scala.collection.mutable.BufferLike
    def +=(elem: X) = {
      require(length < limit)

      innerPut(length, elem)

      size = length + 1
      this
    }
    def +=:(elem: X) = {
      insertAll(0, Seq(elem))
      this
    }
    def clear() = {
      size = 0
    }
    override def ++=(t: TraversableOnce[X]) = {
      innerInsertAll(this.size, t)
      this
    }
    def insertAll(n: Int, elems: Traversable[X]) = innerInsertAll(n, elems)
    private def innerInsertAll(n: Int, elems: TraversableOnce[X]) = {
      val l = elems.toSeq
      require(length + l.size <= limit)

      copyRecords(n, n + l.size, length - n)

      for ((elem, i) <- l.zipWithIndex) {
        innerPut(n + i, elem)
      }

      size = length + l.size

    }
    def remove(n: Int): X = {
      require(n >= 0 && n < length)

      val removed = innerGet(n)

      // Copy
      copyRecords(n + 1, n, length - (n + 1))

      size = length - 1

      removed
    }

    def update(n: Int, newelem: X): Unit  = {
      require(0 <= n && n < length)
      innerPut(n, newelem)
    }

    def space = limit - length
    def full = space > 0

    def iterator: Iterator[X] = new Iter

    private class Iter extends Iterator[X] {
      var index = 0
      def hasNext = index < AbstractSequence.this.length
      def next = {
        require(hasNext)
        try apply(index)
        finally index += 1
      }
    }
  }

  final class VarSequence[X] private[Struct](start: Int, limit: Int)(implicit ser: Serializer[X])
      extends AbstractSequence[X](start, limit, ser.size) {
    def innerGet(idx: Int) = {
      storage.read[X](dataStart + itemSize * idx)
    }
    def innerPut(n: Int, newelem: X): Unit  = {
      storage.write(dataStart + n * itemSize, newelem)
    }
  }

  final class StructSequence[X <: Struct[X]] private[Struct](start: Int, limit: Int)(implicit info: StructInfo[X])
      extends AbstractSequence[X](start, limit, info.size) {
    def innerGet(idx: Int) = {
      info.cons(storage, dataStart + itemSize * idx)
    }
    def innerPut(n: Int, newelem: X): Unit  = {
      innerGet(n) := newelem
    }
  }
}

case class StructInfo[X <: Struct[X]](cons: (Allocator#Storage, Int) => X) {
  val size = cons(null, 0).size
  def allocate(alloc: Allocator) = {
    val storage = alloc(size)
    cons(storage, 0)
  }
  def inMemory = allocate(MemoryAllocator)
  def allocateWithStorage(alloc: Allocator) = {
    val storage = alloc(size)
    (storage, cons(storage, 0))
  }
}