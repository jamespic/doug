package uk.me.jamespic.dougng.util

import scala.collection.mutable.ListBuffer
import scala.collection.BitSet
import scala.annotation.tailrec
import java.util.PriorityQueue
import scala.collection.JavaConversions._

object MapReduceBRTree {
  import Ordering.Implicits._
  // Make these vals once we've optimised them
  var DefaultFanout = 16
  var DefaultBufferSize = 32

  private def mergeLists[T: Ordering](s1: List[T], s2: List[T]) = {
    recRevMerge(s1, s2, Nil).reverse
  }

  private def recRevMerge[T: Ordering](s1: List[T], s2: List[T], accum: List[T]): List[T] = {
    (s1, s2) match {
      case (Nil, s) => s.reverse ++ accum
      case (s, Nil) => s.reverse ++ accum
      case (h1 :: t1, h2 :: t2) =>
        if (h1 <= h2) recRevMerge(t1, s2, h1 :: accum)
        else recRevMerge(s1, t2, h2 :: accum)
    }
  }

  private def splitAround[T: Ordering](l: List[T], e: T) = {
    val left = ListBuffer.empty[T]
    @tailrec def splitRec(s: List[T]): List[T] = s match {
      case h :: t if h <= e =>
        left += h
        splitRec(t)
      case _ => s
    }
    val right = splitRec(l)
    (left.result, right)
  }
}

class MapReduceBRTree[K, V, S]
    (mapReduce: Traversable[V] => Option[S], rereduce: Traversable[S] => Option[S])
    (alloc: Allocator)
    (implicit serk: Serializer[K], ord: Ordering[K], sers: Serializer[S], serv: Serializer[V])
    extends Traversable[(K, V)]{
  import Ordering.Implicits._
  import MapReduceBRTree._

  private val fanout = MapReduceBRTree.DefaultFanout
  private val bufferSize = MapReduceBRTree.DefaultBufferSize
  private implicit val kOrdering = Ordering.by[(K, V), K](_._1)
  private implicit val hSer = alloc.handleSerializer

  private lazy implicit val pointerInfo = StructInfo(new Pointer(_, _))
  private lazy implicit val branchInfo = StructInfo(new Branch(_, _))
  private lazy implicit val leafInfo = StructInfo(new Leaf(_, _))

  private var rootPointer = {
    val (storage, node) = leafInfo.allocateWithStorage(alloc)
    val pointer = pointerInfo.inMemory
    pointer.loc() = storage.handle
    pointer
  }

  def sync[A](f: => A) = alloc.sync(f)

  def +=(entry: (K, V)) = this ++= (entry :: Nil)

  def ++=(entries: TraversableOnce[(K, V)]) = sync {
    val insertion = rootPointer.insert(entries.toList)
    if (insertion.destructive) rootPointer = insertion.buildRoot
  }

  def foreach[U](f: ((K, V)) => U): Unit = {
    doBetween(f, None, None)
  }

  def getBetween(low: Option[K], high: Option[K]): Traversable[(K, V)] = new Traversable[(K, V)] {
    def foreach[U](f: ((K, V)) => U): Unit = doBetween(f, low, high)
  }

  private def doBetween[U](f: ((K, V)) => U, low: Option[K], high: Option[K]): Unit = {
    val queue = new PriorityQueue(64, kOrdering)
    rootPointer.visit(f, low, high, queue)
    while (true) {
      queue.poll() match {
        case null => return
        case e => f(e)
      }
    }
  }

  private sealed trait PointerType
  private case object LeafType extends PointerType
  private case object BranchType extends PointerType

  private trait Visitable {
    def visit[U](f: ((K, V)) => U, low: Option[K], high: Option[K], q: PriorityQueue[(K, V)]): Unit
  }

  private class Pointer(storage: Allocator#Storage, offset: Int)
      extends Struct[Pointer](storage, offset)
      with Visitable {
    private val _typ = __var__[Byte]
    val summary = __var__[Option[S]]
    val buffer = __buffer__[(K, V)](bufferSize)
    val loc = __var__[alloc.HandleType]
    def typ = _typ() match {
      case 0 => LeafType
      case 1 => BranchType
    }
    def typ_=(pt: PointerType) = pt match {
      case LeafType => _typ() = 0
      case BranchType => _typ() = 1
    }

    def visit[U](f: ((K, V)) => U, low: Option[K], high: Option[K], q: PriorityQueue[(K, V)]) = {
      for (e @ (k, _) <- buffer if low.forall(_ <= k) && high.forall(k <= _)) {
        q add e
      }
      val node = typ match {
        case LeafType =>
          val s = alloc.storage(loc(), leafInfo.size)
          new Leaf(s, 0)
        case BranchType =>
          val s = alloc.storage(loc(), branchInfo.size)
          new Branch(s, 0)
      }
      node.visit(f, low, high, q)
    }


    def insert(entries: List[(K, V)]): Insertion = {
      summary() = None
      if (buffer.space >= entries.size) {
        buffer ++= entries
        // If data is empty, then the insertion was in-place
        Insertion(this)
      } else {
        val newEntries = (entries ++ buffer.toList).sorted
        buffer.clear()
    	typ match {
    	  case LeafType =>
    	    val oldStorage = alloc.storage(loc(), leafInfo.size)
    	    val oldNode = new Leaf(oldStorage, 0)
    	    val merged = mergeLists(newEntries, oldNode.data.toList)
    	    oldStorage.free
    	    if (merged.size <= fanout) {
    	      val (newStorage, newNode) = leafInfo.allocateWithStorage(alloc)
    	      newNode.data ++= merged
    	      loc() = newStorage.handle
    	      Insertion(this)
    	    } else {
    	      buildNewLeafNodes(merged)
    	    }
    	  case BranchType =>
    	    val oldStorage = alloc.storage(loc(), branchInfo.size)
    	    val oldNode = new Branch(oldStorage, 0)
    	    val mergedPointers = ListBuffer.empty[Pointer]
    	    val mergedEntries = ListBuffer.empty[(K, V)]
    	    @tailrec def mergeRec(ptrs: List[Pointer], es: List[(K, V)], nes: List[(K, V)]): Unit = {
    	      val ptr :: ptrTail = ptrs
    	      es match {
    	        case e :: tail =>
    	          val (left, right) = splitAround(nes, e)
    	          val Insertion(newPtrs, newData) = ptr.insert(left)
    	          mergedPointers ++= newPtrs
    	          mergedEntries ++= newData
    	          mergedEntries += e
    	          mergeRec(ptrTail, tail, right)
    	        case Nil =>
    	          val Insertion(newPtrs, newData) = ptr.insert(nes)
    	          mergedPointers ++= newPtrs
    	          mergedEntries ++= newData
    	      }
    	    }
    	    mergeRec(oldNode.pointers.toList, oldNode.data.toList, newEntries)
    	    try if (mergedEntries.size <= fanout) {
    	      val (newStorage, newNode) = branchInfo.allocateWithStorage(alloc)
    	      newNode.data ++= mergedEntries
    	      newNode.pointers ++= mergedPointers
    	      loc() = newStorage.handle
    	      Insertion(this)
    	    } else {
    	      buildNewBranchNodes(mergedPointers.result, mergedEntries.result)
    	    }
    	    finally oldStorage.free
    	}
      }
    }
  }

  private def makePivots(totalSize: Int) = {
    val subNodeCount = totalSize / (fanout + 1) + 1
    val subNodeSize = totalSize.toDouble / subNodeCount
    BitSet((for (i <- 1 until subNodeCount) yield {
      (i * subNodeSize).toInt
    }): _*)
  }

  private def buildNewLeafNodes(l: List[(K, V)]) = {
	val totalSize = l.size
    val pivots = makePivots(totalSize)
	val pointers = ListBuffer.empty[Pointer]
	val entries = ListBuffer.empty[(K, V)]
	def newNode = {
	  val (storage, node) = leafInfo.allocateWithStorage(alloc)
	  val pointer = pointerInfo.inMemory
	  pointer.loc() = storage.handle
	  pointer.typ = LeafType
	  pointers += pointer
	  node
	}
	var currentNode = newNode
	for ((entry, i) <- l.zipWithIndex) {
	  if (pivots contains i) {
	    entries += entry
	    currentNode = newNode
	  } else {
	    currentNode.data += entry
	  }
	}
	Insertion(pointers.result, entries.result)
  }

  private def buildNewBranchNodes(ptrs: List[Pointer], es: List[(K, V)]): Insertion = {
    val totalSize = es.size
    val pivots = makePivots(totalSize)
	val pointers = ListBuffer.empty[Pointer]
	val entries = ListBuffer.empty[(K, V)]
    def newNode = {
	  val (storage, node) = branchInfo.allocateWithStorage(alloc)
	  val pointer = pointerInfo.inMemory
	  pointer.loc() = storage.handle
	  pointer.typ = BranchType
	  pointers += pointer
	  node
	}
    var currentNode = newNode
	@tailrec def buildRec(ptrs: List[Pointer], es: List[(K, V)], i: Int): Unit = (ptrs, es) match {
      case (ptr :: ptail, e :: etail) =>
        if (pivots contains i) {
          entries += e
          currentNode = newNode
        } else {
          currentNode.data += e
        }
        currentNode.pointers += ptr
        buildRec(ptail, etail, i + 1)
      case (Nil, Nil) =>
        ()
      case _ =>
        throw new IllegalStateException("Lists should be the same length")

    }
    currentNode.pointers += ptrs.head
    buildRec(ptrs.tail, es, 0)
    Insertion(pointers.result, entries.result)
  }

  private object Insertion {
    def apply(ptr: Pointer): Insertion = Insertion(ptr :: Nil, Nil)
  }

  private case class Insertion(pointers: List[Pointer], data: List[(K, V)]) {
    require(pointers.size == data.size + 1)
    def destructive = pointers.lengthCompare(1) > 0

    def buildRoot: Pointer = {
      if (pointers.size == 1) {
        pointers.head
      } else if (data.size <= fanout) {
        val (storage, node) = branchInfo.allocateWithStorage(alloc)
        node.pointers ++= pointers
        node.data ++= data
        val ptr = pointerInfo.inMemory
        ptr.typ = BranchType
        ptr.loc() = storage.handle
        ptr
      } else {
        buildNewBranchNodes(pointers, data).buildRoot
      }
    }
  }

  private trait VisitableNode extends Visitable {
    final def flushAndVisit[U](e: (K, V), f: ((K, V)) => U, q: PriorityQueue[(K, V)]) = {
      while (!q.isEmpty() && e > q.peek()) {
        f(q.poll())
      }
      f(e)
    }
  }

  private class Branch(storage: Allocator#Storage, offset: Int)
      extends Struct[Branch](storage, offset)
      with VisitableNode {
    val data = __buffer__[(K, V)](fanout)
    val pointers = __struct_buffer__[Pointer](fanout + 1)

    def visit[U](f: ((K, V)) => U, low: Option[K], high: Option[K], q: PriorityQueue[(K, V)]) = {
      val entries = data.toArray
      for ((ptr, i) <- pointers.zipWithIndex) {
        val leftKeyOpt = if (i == 0) None else Some(entries(i - 1)._1)
        val rightOpt = if (i == entries.length) None else Some(entries(i))
        val rightKeyOpt = rightOpt map (_._1)
        val leftEdge = low filter {lowKey => leftKeyOpt forall (nodeKey => nodeKey <= lowKey)}
        val rightEdge = high filter {highKey => rightKeyOpt forall (nodeKey => highKey <= nodeKey)}
        if ((rightKeyOpt.isEmpty || low.isEmpty || low.get <= rightKeyOpt.get) &&
            (leftKeyOpt.isEmpty || high.isEmpty || leftKeyOpt.get <= high.get)) {
          ptr.visit(f, leftEdge, rightEdge, q)
        }
        for (e @ (k, _) <- rightOpt) {
          if ((low forall (_ <= k)) && (high forall (k <= _))) {
            flushAndVisit(e, f, q)
          }
        }
      }
    }
  }

  private class Leaf(storage: Allocator#Storage, offset: Int)
      extends Struct[Leaf](storage, offset)
      with VisitableNode {
    val data = __buffer__[(K, V)](fanout)

    def visit[U](f: ((K, V)) => U, low: Option[K], high: Option[K], q: PriorityQueue[(K, V)]) = {
      for (e @ (k, _) <- data if low.forall(_ <= k) && high.forall(k <= _)) {
        flushAndVisit(e, f, q)
      }
    }
  }
}