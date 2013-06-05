package uk.me.jamespic.dougng.util

import Serializer.sizeof

object MapReduceDigitalTrie {
  /**
   * An alternative representation of Long, to allow byte ordering.
   *
   * Scala represents longs in 2's complement form. This means that when
   * broken down into bytes, the first byte of -1 is 0xFF, and the first byte of
   * 0 is 0x00, so it looks like -1 is greater than 0. To rectify this, we represent
   * longs differently. We the subtract Long.MinValue from our value, to get its
   * byte-ordered form. This means that the lowest long is 0x0000000000000000 and the
   * highest is 0xFFFFFFFFFFFFFFFF.
   */
  class ByteOrderedLong(val ol: Long) extends AnyVal {
    def toLong = ol + Long.MinValue
    /**
     * Get a given byte. Significance goes from 0 (least significant) to 7 (most significant)
     *
     * Bytes are actually returned as Shorts, since Bytes are signed, and we want unsigned.
     */
    def getByte(sig: Byte) = if (0 <= sig && sig <= 7) {
      UByte((ol >>> (sig * 8)).toByte)
    } else UByte(0)
  }

  implicit def long2ByteOrdered(l: Long) = new ByteOrderedLong(l - Long.MinValue)
  implicit def byteOrdered2Long(bo: ByteOrderedLong) = bo.toLong

  case class UByte(val by: Byte) extends AnyVal {
    def toShort = if (by >= 0) by.toShort else (by.toShort + 0x100).toShort
    def +(i: Int) = i + toShort
    def +(l: Long) = l + toShort
    def *(i: Int) = i * toShort
    def *(l: Long) = l * toShort
    override def toString = toShort.toString
  }

  implicit class IntOps(val i: Int) extends AnyVal {
    def +(ub: UByte) = i + ub.toShort
    def *(ub: UByte) = i * ub.toShort
    def -(ub: UByte) = i - ub.toShort
  }

  implicit class LongOps(val l: Long) extends AnyVal {
    def +(ub: UByte) = l + ub.toShort
    def *(ub: UByte) = l * ub.toShort
    def -(ub: UByte) = l - ub.toShort
  }

  object UByte {
    implicit val serializer = Serializer.caseClassSerializer(UByte.apply _, UByte.unapply _)
  }

  implicit object UByteOrdering extends Ordering[UByte] {
    def compare(x: UByte, y: UByte) = x.toShort compare y.toShort
  }

  private val MultiNodeSize = 14
  private val HashBucketNodeSize = 20
  private val FullNodeSize = 256
}

class MapReduceDigitalTrie[V, S]
    (map: V => S, reduce: Traversable[S] => Option[S])
    (alloc: Allocator)
    (implicit sers: Serializer[S], serv: Serializer[V]) extends Hibernatable {
  import MapReduceDigitalTrie._

  override def sync[B](f: => B) = alloc.sync(f)
  def hibernate() = alloc.hibernate()

  private trait TaggedValue {def interesting: Boolean}
  private object Interesting {def unapply(t: TaggedValue) = if (t.interesting) Some(t) else None}

  private case class Pointer(typ: Byte, loc: alloc.HandleType) extends TaggedValue {
    def apply(): Node = typ match {
      case 0 => new EmptyBranchNode
      case 1 => new SingleBranchNode(alloc.storage(loc)(SingleNode.branchSerializer))
      case 2 => new MultiBranchNode(alloc.storage(loc)(MultiNode.branchSerializer))
      case 3 => ??? // HashBucketBranchNode
      case 4 => new FullBranchNode(alloc.storage(loc)(FullNode.branchSerializer))
      case 5 => new EmptyLeafNode
      case 6 => new SingleLeafNode(alloc.storage(loc)(SingleNode.leafSerializer))
      case 7 => new MultiLeafNode(alloc.storage(loc)(MultiNode.leafSerializer))
      case 8 => ??? // HashBucketLeafNode
      case 9 => new FullLeafNode(alloc.storage(loc)(FullNode.leafSerializer))
    }

    def interesting = (typ != 0.toByte) && (typ != 5.toByte)
  }

  private case class LList[X: Serializer](interesting: Boolean, x: X, rest: alloc.HandleType) extends TaggedValue with Traversable[X] {
    override def foreach[U](f: X => U) = {
      var current: LList[X] = this
      while (current.interesting) {
        f(current.x)
        current = alloc.storage(current.rest)(LList.serializer[X]).read[LList[X]](0)
      }
    }
  }

  private object LList {
    implicit val handleSerializer = alloc.handleSerializer
    implicit def serializer[X: Serializer] = {
      Serializer.caseClassSerializer(LList.apply[X] _, LList.unapply[X] _)
    }
  }

  private object Pointer {
    implicit val handleSerializer = alloc.handleSerializer
    implicit val pointerSerializer = Serializer.caseClassSerializer(Pointer.apply _, Pointer.unapply _)
  }

  /*
   * Base type
   */
  private trait Node {
    val pointer: Pointer
    def summary: Option[S]
    def invalidate(): Unit
  }

  /*
   *  Base Implementation types
   */
  private abstract class BaseNode[X: Serializer](val typ: Byte) extends Traversable[(UByte, X)] with Node {
    type SemanticType <: BaseNode[X]
    def apply(idx: UByte): Option[X]
    protected def getOrElseInsert(idx: UByte, orElse: => X): (Option[Pointer], X)
    protected def insertOrElseUpdate(idx: UByte, absent: => X, present: X => X): (Option[Pointer], X)
    protected def upgrade(entries: Traversable[(UByte, X)]): SemanticType
    protected def upgradeAndLoad(extraIndex: UByte, extraVal: => X) = {
      val x = extraVal
      var newNode = upgrade(this ++ Seq((extraIndex, extraVal)))
      (Some(newNode.pointer), x)
    }
  }

  private abstract class WithStorage[X: Serializer](typ: Byte, val storage: alloc.Storage)
      extends BaseNode[X](typ) {
    type Ser // type that represents the serialized representation of this class
    val pointer = Pointer(typ, storage.handle)
    // WithStorage subclasses must store their summaries as the first element of their storage
    def summary = storage.read[Option[S]](0)
    def summary_=(s: Option[S]) = storage.write(0, s)
    def invalidate() = {summary = None}
    override def upgradeAndLoad(idx: UByte, extraVal: => X) = {
      val ret = super.upgradeAndLoad(idx, extraVal)
      storage.free
      ret
    }
    protected def bulkLoad(entries: Traversable[(UByte, X)]): Unit
  }

  private abstract class EmptyNode[X: Serializer](typ: Byte) extends BaseNode[X](typ) {
    val pointer = Pointer(typ, alloc.nullHandle)
    def summary = None
    def invalidate() = ()
    def apply(idx: UByte) = None
    override def foreach[U](f: ((UByte, X)) => U) = ()
    protected def getOrElseInsert(idx: UByte, orElse: => X) = upgradeAndLoad(idx, orElse)
    protected def insertOrElseUpdate(idx: UByte, absent: => X, present: X => X) = upgradeAndLoad(idx, absent)
  }

  private object SingleNode {
    implicit def serializer[X: Serializer] =
      implicitly[Serializer[(Option[S], UByte, X)]]
    val branchSerializer = serializer[Pointer]
    val leafSerializer = serializer[LList[S]]
  }

  private abstract class SingleNode[X: Serializer](typ: Byte, storage: alloc.Storage)
      extends WithStorage[X](typ, storage) {
    type Ser = (Option[S], UByte, X)
    private var dataOpt: Option[(UByte, X)] = None

    private def data = dataOpt match {
      case Some((index, x)) =>
        (index, x)
      case None =>
        val d = storage.read[(UByte, X)](sizeof[Option[S]]) // Skip summary, at start
        dataOpt = Some(d)
        d
    }
    private def index = data._1
    private def x = data._2

    def apply(idx: UByte) = if (idx == index) Some(x) else None
    override def foreach[U](f: ((UByte, X)) => U) = f((index, x))
    protected def getOrElseInsert(idx: UByte, orElse: => X) = {
      if (idx == index) {
        (None, x)
      } else {
        upgradeAndLoad(idx, orElse)
      }
    }

    protected def insertOrElseUpdate(idx: UByte, absent: => X, present: X => X) = {
      if (idx == index) {
        val newX = present(x)
        storage.write(sizeof[(Option[S],UByte)], newX) // skip summary, and UByte, at start
        dataOpt = Some((idx, newX))
        (None, newX)
      } else {
        upgradeAndLoad(idx, absent)
      }
    }

    protected def bulkLoad(entries: Traversable[(UByte, X)]) = {
      require(entries.size == 1)
      storage.write(sizeof[Option[S]], entries.head) // Skip summary at start
    }
  }

  private object MultiNode {
    implicit def serializer[X: Serializer] =
      new Serializer.VariableCollectionSerializer[(UByte, X), IndexedSeq[(UByte, X)]](MultiNodeSize)
    val branchSerializer = serializer[Pointer]
    val leafSerializer = serializer[LList[S]]
  }

  //TODO: Figure out if we can refactor this to take advantage of TaggedValue
  private abstract class MultiNode[X: Serializer](typ: Byte, storage: alloc.Storage)
      extends WithStorage[X](typ, storage) {
    import MultiNode._
    type Ser = (Option[S], IndexedSeq[(UByte, X)])
    implicit val ser = serializer[X]
    protected var dataOpt: Option[IndexedSeq[(UByte, X)]] = None
    type Rec = (Byte, UByte, X)

    private def data = dataOpt match {
      case Some(l) => l
      case None =>
        val l = storage.read[IndexedSeq[(UByte, X)]](sizeof[Option[S]]) // Skip summary at start
        dataOpt = Some(l)
        l
    }

    def apply(idx: UByte) = data find {case (i, x) => idx == i } map (_._2)
    override def foreach[U](f: ((UByte, X)) => U) = data.sortBy(_._1).foreach(f)

    protected def getOrElseInsert(idx: UByte, orElse: => X) = {
      apply(idx) match {
        case Some(x) => (None, x)
        case None =>
          addToEnd(idx, orElse)
      }
    }

    protected def insertOrElseUpdate(idx: UByte, absent: => X, present: X => X) = {
      val pos = data indexWhere {case (i, x) => i == idx}
      pos match {
        case -1 =>
          addToEnd(idx, absent)
        case _ =>
          // Found
          val oldX = data(pos)._2
          val newX = present(oldX)
          storage.write(sizeof[Option[S]] + pos * sizeof[Rec], (1.toByte, idx, newX)) // Skip summary at start
          dataOpt = Some(data.updated(pos, (idx, newX)))
          (None, newX)
      }
    }

    private def addToEnd(idx: UByte, x: X) = {
      val currentSize = data.size
      if (currentSize >= MultiNodeSize) {
        upgradeAndLoad(idx, x)
      } else {
        storage.write(sizeof[Option[S]] + currentSize * sizeof[Rec], (1.toByte, idx, x))
        dataOpt = dataOpt map (l => l :+ (idx, x))
        (None, x)
      }
    }

    protected def bulkLoad(entries: Traversable[(UByte, X)]) = {
      val seq = entries.toIndexedSeq: IndexedSeq[(UByte, X)]
      dataOpt = Some(seq)
      storage.write(sizeof[Option[S]], seq)
    }

  }


  //TODO: Implement this
  private abstract class HashBucketNode[X <: TaggedValue: Serializer](typ: Byte, storage: alloc.Storage)
      extends WithStorage[X](typ, storage) {
    def apply(idx: UByte) = ???
    override def foreach[U](f: ((UByte, X)) => U) = ???
    protected def getOrElseInsert(idx: UByte, orElse: => X) = ???
    protected def insertOrElseUpdate(idx: UByte, absent: => X, present: X => X) = ???
    protected def bulkLoad(entries: Traversable[(UByte, X)]) = ???
  }

  private object FullNode {
    implicit def serializer[X: Serializer] =
      new Serializer.FixedCollectionSerializer[X, IndexedSeq[X]](FullNodeSize)
    val branchSerializer = serializer[Pointer]
    val leafSerializer = serializer[LList[S]]
  }

  private abstract class FullNode[X <: TaggedValue : Serializer](typ: Byte, storage: alloc.Storage)
      extends WithStorage[X](typ, storage) {
    import FullNode._
    type Ser = (Option[S], IndexedSeq[X])
    type Rec = X
    private implicit val ser = serializer[X]

    def apply(idx: UByte) = {
      val x = storage.read[X](sizeof[Option[S]] + idx * sizeof[Rec])
      if (x.interesting) Some(x) else None
    }

    override def foreach[U](f: ((UByte, X)) => U) = {
      val records = storage.read(sizeof[Option[S]])(ser).zipWithIndex
      for ((x, i) <- records; if x.interesting) f((UByte(i.toByte), x))
    }

    protected def getOrElseInsert(idx: UByte, orElse: => X) = {
      val ret = apply(idx) getOrElse {
        val x = orElse
        storage.write[X](sizeof[Option[S]] + idx * sizeof[Rec], x)
        x
      }
      (None, ret)
    }

    protected def insertOrElseUpdate(idx: UByte, absent: => X, present: X => X) = {
      val newX = apply(idx) match {
        case Some(x) => present(x)
        case None => absent
      }
      storage.write[X](sizeof[Option[S]] + idx * sizeof[Rec], newX)
      (None, newX)
    }

    protected def bulkLoad(entries: Traversable[(UByte, X)]) = {
      for ((i, x) <- entries) {
        storage.write[X](sizeof[Option[S]] + i * sizeof[Rec], x)
      }
      ???
    }
    protected def upgrade(entries: Traversable[(UByte, X)]) =
      throw new Exception("It should not be necessary to upgrade a full node")
  }

  /*
   * Semantic types
   */

  private trait LeafNode extends BaseNode[LList[V]] {
    type SemanticType = LeafNode
  }

  private trait BranchNode extends BaseNode[Pointer] {
    type SemanticType = BranchNode
  }

  /*
   * Concrete types
   */
  private class EmptyBranchNode extends EmptyNode[Pointer](0) with BranchNode {
    def upgrade(entries: Traversable[(UByte, Pointer)]) = {
      new SingleBranchNode(entries)
    }
  }

  private class SingleBranchNode(storage: alloc.Storage)
      extends SingleNode[Pointer](1, storage) with BranchNode {
    def this(entries: Traversable[(UByte, Pointer)]) {
      this(alloc(SingleNode.branchSerializer))
      bulkLoad(entries)
    }

    def upgrade(entries: Traversable[(UByte, Pointer)]) = {
      new MultiBranchNode(storage)
    }
  }

  private class MultiBranchNode(storage: alloc.Storage)
      extends MultiNode[Pointer](2, storage) with BranchNode {
    def this(entries: Traversable[(UByte, Pointer)]) {
      this(alloc(MultiNode.branchSerializer))
      bulkLoad(entries)
    }
    def upgrade(entries: Traversable[(UByte, Pointer)]) = ???
  }

  private class HashBucketBranchNode(storage: alloc.Storage)
      extends HashBucketNode[Pointer](3, storage) with BranchNode {
    def upgrade(entries: Traversable[(UByte, Pointer)]) = ???
  }


  private class FullBranchNode(storage: alloc.Storage)
      extends FullNode[Pointer](4, storage) with BranchNode {
    def this(entries: Traversable[(UByte, Pointer)]) {
      this(alloc(FullNode.branchSerializer))
      bulkLoad(entries)
    }
  }

  private class EmptyLeafNode extends EmptyNode[LList[V]](5) with LeafNode {
    def upgrade(entries: Traversable[(UByte, LList[V])]) = {
      new SingleLeafNode(entries)
    }
  }

  private class SingleLeafNode(storage: alloc.Storage)
      extends SingleNode[LList[V]](6, storage) with LeafNode {
    def this(entries: Traversable[(UByte, LList[V])]) {
      this(alloc(SingleNode.leafSerializer))
      bulkLoad(entries)
    }
    def upgrade(entries: Traversable[(UByte, LList[V])]) = {
      new MultiLeafNode(entries)
    }
  }

  private class MultiLeafNode(storage: alloc.Storage)
      extends MultiNode[LList[V]](7, storage) with LeafNode {
    def this(entries: Traversable[(UByte, LList[V])]) {
      this(alloc(MultiNode.leafSerializer))
      bulkLoad(entries)
    }
    def upgrade(entries: Traversable[(UByte, LList[V])]) = ???
  }

  private class HashBucketLeafNode(storage: alloc.Storage)
      extends HashBucketNode[LList[V]](8, storage) with LeafNode {
    def upgrade(entries: Traversable[(UByte, LList[V])]) = ???
  }

  private class FullLeafNode(storage: alloc.Storage)
      extends FullNode[LList[V]](9, storage) with LeafNode {
    def this(entries: Traversable[(UByte, LList[V])]) {
      this(alloc(FullNode.leafSerializer))
      bulkLoad(entries)
    }
  }

}