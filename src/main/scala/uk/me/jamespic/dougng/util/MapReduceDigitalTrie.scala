package uk.me.jamespic.dougng.util

import Serializer.sizeof
import scala.annotation.tailrec

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
    def getByte(sig: Int) = if (0 <= sig && sig <= 7) {
      UByte((ol >>> (sig * 8)).toByte)
    } else UByte(0)

    def bytes = for (i <- 0 to 7) yield getByte(i.toByte)

    def matchAtLevel(level: Int, o: ByteOrderedLong) = {
      if (level > 7) true
      else {
        val mask = (-1L) << (level * 8)
        ((ol ^ o.ol) & mask) == 0
      }
    }
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
  private val HashBucketSize = 20
  private val HashBucketCount = 4
  private val FullNodeSize = 256
}

class MapReduceDigitalTrie[V, S]
    (mapReduce: Traversable[V] => Option[S], rereduce: Traversable[S] => Option[S])
    (alloc: Allocator)
    (implicit sers: Serializer[S], serv: Serializer[V]) extends Hibernatable {
  import MapReduceDigitalTrie._

  private var rootNode = TaggedPointer((new EmptyBranchNode).pointer, false)
  private var lastInsertion: Option[SearchState] = None

  def +=(e: (Long, V)) = sync {
    val (k, v) = e
    lastInsertion match {
      case Some(SearchState(level, address, pointer))
           if k.matchAtLevel(level, address) && pointer().summary.isEmpty =>
        insert(SearchState(level, k, pointer), v)
      case _ =>
        lastInsertion = None
        val newRootOpt = insert(SearchState(7, k, rootNode), v)
        for (newRoot <- newRootOpt) {
          rootNode = newRoot
        }
    }
    ()
  }

  def prettyPrint: String = sync {
    prettyPrint(0, UByte(0.toByte), rootNode()).mkString("\n")
  }

  private def prettyPrint(depth: Int, idx: UByte, node: Node): Traversable[String] = {
    val head = Traversable(s"${"|" * depth}+ [$idx] ${node.getClass.getSimpleName} - Summary: ${node.summary}")
    val tail = node match {
      case branch: BranchNode =>
        branch flatMap {case (subIdx, ptr) =>
          prettyPrint(depth + 1, subIdx, ptr())
        }
      case leaf: LeafNode =>
        for ((subIdx, vlist) <- leaf; v <- vlist) yield {
          s"${"|" * (depth + 1)}+ [$subIdx] $v"
        }
    }
    head ++ tail
  }

  private def insert(state: SearchState, value: V): Option[TaggedPointer] = {
    val SearchState(level, address, pointer) = state
    val node = pointer()
    val idx = address.getByte(level)
    val newPtrOpt = node match {
      case branch: BranchNode if level > 0 =>
        val idx = address.getByte(level)
        branch.visit(idx, Some(generateTower(level - 1, address, value)),
          {ptr =>
            lastInsertion = Some(state)
            val nextNode = ptr()
            val nextState = SearchState(level - 1, address, ptr)
            insert(nextState, value)
          }
        )
      case leaf: LeafNode if level == 0 =>
        leaf.visit(idx,
            Some(LList(LList.NoNextTyp, value, alloc.nullHandle)),
            {case LList(oldTyp, oldValue, oldHandle) =>
              lastInsertion = Some(state)
              val next = LList(oldTyp, oldValue, oldHandle)
              val storage = alloc(LList.serializer[V])
              storage.write(0, next)
              Some(LList(LList.HasNextTyp, value, storage.handle))
            }
        )
    }
    newPtrOpt match {
      case Some(newPtr) =>
        // Assume new pointers have no summaries, alternatively, uncomment next line
        Some(TaggedPointer(newPtr, newPtr().summary.isDefined))
        //Some(TaggedPointer(newPtr, false))
      case None if pointer.hasSummary =>
        node.invalidate()
        Some(TaggedPointer(pointer.pointer, false))
      case _ => None
    }
  }

  private def generateTower(level: Int, address: ByteOrderedLong, value: V): TaggedPointer = {
    level match {
      case 0 =>
        TaggedPointer(
            new SingleLeafNode(Seq(address.getByte(0) -> LList(LList.NoNextTyp, value, alloc.nullHandle))).pointer,
            false
         )
      case i if i > 0 =>
        TaggedPointer(
          new SingleBranchNode(Seq(address.getByte(level) -> generateTower(level - 1, address, value))).pointer,
          false
        )
    }
  }

  override def sync[B](f: => B) = alloc.sync(f)
  def hibernate() = alloc.hibernate()

  private case class SearchState(level: Int, address: ByteOrderedLong, pointer: TaggedPointer)

  private trait TaggedValue {def interesting: Boolean}
  private object Interesting {def unapply(t: TaggedValue) = if (t.interesting) Some(t) else None}

  private case class Pointer(typ: Byte, loc: alloc.HandleType) extends TaggedValue {
    def apply(): Node = typ match {
      case 0 => new EmptyBranchNode
      case 1 => new SingleBranchNode(alloc.storage(loc)(SingleNode.branchSerializer))
      case 2 => new MultiBranchNode(alloc.storage(loc)(MultiNode.branchSerializer))
      case 3 => new HashBucketBranchNode(alloc.storage(loc)(HashBucketNode.branchSerializer))
      case 4 => new FullBranchNode(alloc.storage(loc)(FullNode.branchSerializer))
      case 5 => new EmptyLeafNode
      case 6 => new SingleLeafNode(alloc.storage(loc)(SingleNode.leafSerializer))
      case 7 => new MultiLeafNode(alloc.storage(loc)(MultiNode.leafSerializer))
      case 8 => new HashBucketLeafNode(alloc.storage(loc)(HashBucketNode.leafSerializer))
      case 9 => new FullLeafNode(alloc.storage(loc)(FullNode.leafSerializer))
    }

    def interesting = (typ != 0.toByte) && (typ != 5.toByte)
  }

  private object Pointer {
    implicit val handleSerializer = alloc.handleSerializer
    implicit val pointerSerializer = Serializer.caseClassSerializer(Pointer.apply _, Pointer.unapply _)
  }

  private case class TaggedPointer(pointer: Pointer, hasSummary: Boolean) extends TaggedValue {
    def apply() = pointer.apply()
    def interesting = pointer.interesting
  }

  private object TaggedPointer {
    implicit val serializer = Serializer.caseClassSerializer(TaggedPointer.apply _, TaggedPointer.unapply _)
  }

  private case class LList[X: Serializer](typ: Byte, x: X, rest: alloc.HandleType) extends TaggedValue with Traversable[X] {
    override def foreach[U](f: X => U) = {
      LList.foreach(this, f)
    }

    def interesting = typ > 0
  }

  private object LList {
    val NilTyp = 0.toByte
    val NoNextTyp = 1.toByte
    val HasNextTyp = 2.toByte
    implicit val handleSerializer = alloc.handleSerializer
    implicit def serializer[X: Serializer] = {
      Serializer.caseClassSerializer(LList.apply[X] _, LList.unapply[X] _)
    }
    @tailrec def foreach[X: Serializer, U](llist: LList[X], f: X => U) {
      llist.typ match {
        case NilTyp =>
          ()
        case NoNextTyp =>
          f(llist.x)
        case HasNextTyp =>
          f(llist.x)
          val next = alloc.storage(llist.rest)(LList.serializer[X]).read[LList[X]](0)
          foreach(next, f)
      }
    }
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
  private abstract class BaseNode[X <: TaggedValue : Serializer](val typ: Byte) extends Traversable[(UByte, X)] with Node {
    type SemanticType <: BaseNode[X]
    protected def summary_=(s: Option[S])
    def apply(idx: UByte): Option[X]
    //def getOrElseInsert(idx: UByte, absent: => X, present: X => Unit): Option[Pointer]
    def visit(idx: UByte, absent: => Option[X], present: X => Option[X]): Option[Pointer]
    protected def upgrade(entries: Traversable[(UByte, X)]): SemanticType
    protected def upgradeAndLoad(extraIndex: UByte, extraVal: => X) = {
      var newNode = upgrade(this ++ Seq((extraIndex, extraVal)))
      Some(newNode.pointer)
    }
  }

  private abstract class WithStorage[X <: TaggedValue: Serializer](typ: Byte, val storage: alloc.Storage)
      extends BaseNode[X](typ) {
    type Ser // type that represents the serialized representation of this class
    val pointer = Pointer(typ, storage.handle)
    // WithStorage subclasses must store their summaries as the first element of their storage
    def summary = storage.read[Option[S]](0)
    protected override def summary_=(s: Option[S]) = storage.write(0, s)
    def invalidate() = {summary = None}
    override def upgradeAndLoad(idx: UByte, extraVal: => X) = {
      val ret = super.upgradeAndLoad(idx, extraVal)
      storage.free
      ret
    }
    protected def bulkLoad(entries: Traversable[(UByte, X)]): Unit
  }

  private abstract class EmptyNode[X <: TaggedValue : Serializer](typ: Byte) extends BaseNode[X](typ) {
    val pointer = Pointer(typ, alloc.nullHandle)
    def summary: Option[S] = None
    protected override def summary_=(s: Option[S]) = require(s == None)
    def invalidate() = ()
    def apply(idx: UByte) = None
    override def foreach[U](f: ((UByte, X)) => U) = ()
    def visit(idx: UByte, absent: => Option[X], present: X => Option[X]) = absent flatMap (newX => upgradeAndLoad(idx, newX))
  }

  private trait MetaData[NodeType[_ <: TaggedValue] <: WithStorage[_]] {
    def serializer[X <: TaggedValue: Serializer]: Serializer[NodeType[X]#Ser]
    val branchSerializer = serializer[TaggedPointer]
    val leafSerializer = serializer[LList[V]]
    object Implicits {
      implicit def ser[X <: TaggedValue : Serializer] = serializer[X]
    }
  }

  private object SingleNode extends MetaData[SingleNode] {
    def serializer[X <: TaggedValue: Serializer] = implicitly
  }

  private abstract class SingleNode[X <: TaggedValue : Serializer](typ: Byte, storage: alloc.Storage)
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

    def visit(idx: UByte, absent: => Option[X], present: X => Option[X]) = {
      if (idx == index) {
        for (newX <- present(x)) {
          storage.write(sizeof[(Option[S],UByte)], newX) // skip summary, and UByte, at start
          dataOpt = Some((idx, newX))
        }
        None
      } else {
        absent flatMap (newX => upgradeAndLoad(idx, newX))
      }
    }

    protected def bulkLoad(entries: Traversable[(UByte, X)]) = {
      require(entries.size == 1)
      storage.write(sizeof[Option[S]], entries.head) // Skip summary at start
    }
  }

  private object MultiNode extends MetaData[MultiNode] {
    implicit def collSerializer[X: Serializer]: Serializer[IndexedSeq[(UByte, X)]] =
      new Serializer.VariableCollectionSerializer[(UByte, X), IndexedSeq[(UByte, X)]](MultiNodeSize)
    def serializer[X <: TaggedValue: Serializer] = implicitly
  }

  //TODO: Figure out if we can refactor this to take advantage of TaggedValue
  private abstract class MultiNode[X <: TaggedValue : Serializer](typ: Byte, storage: alloc.Storage)
      extends WithStorage[X](typ, storage) {
    import MultiNode._
    import Implicits._
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

    def visit(idx: UByte, absent: => Option[X], present: X => Option[X]) = {
      val pos = data indexWhere {case (i, x) => i == idx}
      pos match {
        case -1 =>
          absent flatMap (newX => addToEnd(idx, newX))
        case _ =>
          // Found
          val oldX = data(pos)._2
          for (newX <- present(oldX)) {
            storage.write(sizeof[Option[S]] + pos * sizeof[Rec], (1.toByte, idx, newX)) // Skip summary at start
            dataOpt = Some(data.updated(pos, (idx, newX)))
          }
          None
      }
    }

    private def addToEnd(idx: UByte, x: X) = {
      val currentSize = data.size
      if (currentSize >= MultiNodeSize) {
        upgradeAndLoad(idx, x)
      } else {
        storage.write(sizeof[Option[S]] + currentSize * sizeof[Rec], (1.toByte, idx, x))
        dataOpt = dataOpt map (l => l :+ (idx, x))
        None
      }
    }

    protected def bulkLoad(entries: Traversable[(UByte, X)]) = {
      val seq = entries.toIndexedSeq: IndexedSeq[(UByte, X)]
      dataOpt = Some(seq)
      storage.write(sizeof[Option[S]], seq)
    }

  }

  private class LowPriorityHashBucketImplicits {
    type Bucket[X] = IndexedSeq[(UByte, X)]
    implicit def bucketSerializer[X: Serializer]: Serializer[Bucket[X]] =
      new Serializer.VariableCollectionSerializer[(UByte, X), IndexedSeq[(UByte, X)]](HashBucketSize)

  }

  private object HashBucketNode extends LowPriorityHashBucketImplicits with MetaData[HashBucketNode] {
    implicit def collSerializer[X: Serializer]: Serializer[IndexedSeq[Bucket[X]]] =
      (new Serializer.FixedCollectionSerializer
            [Bucket[X], IndexedSeq[Bucket[X]]]
            (HashBucketCount))
    def serializer[X <: TaggedValue: Serializer] = implicitly
    def hash(ub: UByte) = {
      val x = ((ub.by >>> 4) ^ ub.by) & 0x0F
      ((x >>> 2) ^ x) & 0x03
    }
  }

  private abstract class HashBucketNode[X <: TaggedValue : Serializer](typ: Byte, storage: alloc.Storage)
      extends WithStorage[X](typ, storage) {
    import HashBucketNode._
    import Implicits._
    type Bucket = IndexedSeq[(UByte, X)]
    type Ser = (Option[S], IndexedSeq[Bucket])
    private def bucket(idx: UByte) = {
      val h = hash(idx)
      val data = storage.read[Bucket](sizeof[Option[S]] + h * sizeof[Bucket])
      (h, data)
    }
    def apply(idx: UByte) = {
      val (h, data) = bucket(idx)
      data find {case (i, x) => i == idx} map (_._2)
    }
    override def foreach[U](f: ((UByte, X)) => U) = {
      val buckets = storage.read[IndexedSeq[Bucket]](sizeof[Option[S]])
      buckets.flatten.sortBy(_._1).foreach(f)
    }

    def visit(idx: UByte, absent: => Option[X], present: X => Option[X]) = {
      val (h, data) = bucket(idx)
      val pos = data indexWhere {case (i, x) => i == idx}
      pos match {
        case -1 => absent flatMap (newX => addToBucket(h, data, (idx, newX)))
        case p =>
          val oldX = data(p)
          for (newX <- present(oldX._2)) {
            writeToPos(h, p, (idx, newX))
          }
          None
      }
    }

    private def addToBucket(h: Int, data: Bucket, value: (UByte, X)) = {
      val currentSize = data.size
      if (currentSize >= HashBucketSize) {
        upgradeAndLoad(value._1, value._2)
      } else {
        writeToPos(h, currentSize, value)
        None
      }
    }

    private def writeToPos(h: Int, pos: Int, value: (UByte, X)) = {
      storage.write(sizeof[Option[S]] + h * sizeof[Bucket] +  pos * sizeof[(Byte, UByte, X)], (1.toByte, value._1, value._2))
    }

    protected def bulkLoad(entries: Traversable[(UByte, X)]) = {
      val groups = entries groupBy {case (i, x) => hash(i)}
      val buckets: IndexedSeq[Bucket] = for (i <- 0 to 3) yield {
        groups.getOrElse(i, Traversable.empty[(UByte, X)]).toIndexedSeq
      }
      storage.write(sizeof[Option[S]], buckets)
    }
  }

  private object FullNode extends MetaData[FullNode] {
    implicit def collSerializer[X: Serializer]: Serializer[IndexedSeq[X]] =
      new Serializer.FixedCollectionSerializer[X, IndexedSeq[X]](FullNodeSize)
    def serializer[X <: TaggedValue: Serializer] = implicitly

  }

  private abstract class FullNode[X <: TaggedValue : Serializer](typ: Byte, storage: alloc.Storage)
      extends WithStorage[X](typ, storage) {
    import FullNode._
    import Implicits._
    type Ser = (Option[S], IndexedSeq[X])
    type Rec = X
    private implicit val ser = collSerializer[X]

    def apply(idx: UByte) = {
      val x = storage.read[X](sizeof[Option[S]] + idx * sizeof[Rec])
      if (x.interesting) Some(x) else None
    }

    override def foreach[U](f: ((UByte, X)) => U) = {
      val records = storage.read(sizeof[Option[S]])(ser).zipWithIndex
      for ((x, i) <- records; if x.interesting) f((UByte(i.toByte), x))
    }

    def visit(idx: UByte, absent: => Option[X], present: X => Option[X]) = {
      val newXOpt = apply(idx) match {
        case Some(x) => present(x)
        case None => absent
      }
      for (newX <- newXOpt) {
        storage.write[X](sizeof[Option[S]] + idx * sizeof[Rec], newX)
      }
      None
    }

    protected def bulkLoad(entries: Traversable[(UByte, X)]) = {
      for ((i, x) <- entries) {
        storage.write[X](sizeof[Option[S]] + i * sizeof[Rec], x)
      }
    }
    protected def upgrade(entries: Traversable[(UByte, X)]) =
      throw new Exception("It should not be necessary to upgrade a full node")
  }

  /*
   * Semantic types
   */

  private trait LeafNode extends BaseNode[LList[V]] {
    type SemanticType = LeafNode
    //abstract override def summary = {
    //  super.summary match {
    //    case Some(s) => Some(s)
    //    case None =>
    //      val values = this flatMap (_._2)
    //      val s = mapReduce(values)
    //      summary = s
    //      s
    //  }
    //}
  }

  private trait BranchNode extends BaseNode[TaggedPointer] {
    type SemanticType = BranchNode
    //abstract override def summary = {
    //  super.summary match {
    //    case Some(s) => Some(s)
    //    case None =>
    //      val s = rereduce(this flatMap {case (i, p) => p().summary})
    //      summary = s
    //      s
    //  }
    //}
  }

  /*
   * Concrete types
   */
  private class EmptyBranchNode extends EmptyNode[TaggedPointer](0) with BranchNode {
    def upgrade(entries: Traversable[(UByte, TaggedPointer)]) = {
      new SingleBranchNode(entries)
    }
  }

  private class SingleBranchNode(storage: alloc.Storage)
      extends SingleNode[TaggedPointer](1, storage) with BranchNode {
    def this(entries: Traversable[(UByte, TaggedPointer)]) {
      this(alloc(SingleNode.branchSerializer))
      bulkLoad(entries)
    }

    def upgrade(entries: Traversable[(UByte, TaggedPointer)]) = {
      new MultiBranchNode(entries)
    }
  }

  private class MultiBranchNode(storage: alloc.Storage)
      extends MultiNode[TaggedPointer](2, storage) with BranchNode {
    def this(entries: Traversable[(UByte, TaggedPointer)]) {
      this(alloc(MultiNode.branchSerializer))
      bulkLoad(entries)
    }
    def upgrade(entries: Traversable[(UByte, TaggedPointer)]) = {
      new HashBucketBranchNode(entries)
    }
  }

  private class HashBucketBranchNode(storage: alloc.Storage)
      extends HashBucketNode[TaggedPointer](3, storage) with BranchNode {
    def this(entries: Traversable[(UByte, TaggedPointer)]) {
      this(alloc(HashBucketNode.branchSerializer))
      bulkLoad(entries)
    }
    def upgrade(entries: Traversable[(UByte, TaggedPointer)]) = {
      new FullBranchNode(entries)
    }
  }


  private class FullBranchNode(storage: alloc.Storage)
      extends FullNode[TaggedPointer](4, storage) with BranchNode {
    def this(entries: Traversable[(UByte, TaggedPointer)]) {
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
    def upgrade(entries: Traversable[(UByte, LList[V])]) = {
      new HashBucketLeafNode(entries)
    }
  }

  private class HashBucketLeafNode(storage: alloc.Storage)
      extends HashBucketNode[LList[V]](8, storage) with LeafNode {
    def this(entries: Traversable[(UByte, LList[V])]) {
      this(alloc(HashBucketNode.leafSerializer))
      bulkLoad(entries)
    }
    def upgrade(entries: Traversable[(UByte, LList[V])]) = {
      new FullLeafNode(entries)
    }
  }

  private class FullLeafNode(storage: alloc.Storage)
      extends FullNode[LList[V]](9, storage) with LeafNode {
    def this(entries: Traversable[(UByte, LList[V])]) {
      this(alloc(FullNode.leafSerializer))
      bulkLoad(entries)
    }
  }

}