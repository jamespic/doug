package uk.me.jamespic.dougng.util

import shapeless._
import scala.collection.mutable.ArrayBuffer
import java.util.Random
import scala.annotation.tailrec

object MapReduceSkipList {
  import Serializer.{caseClassSerializer, MultiSerializer}
  val DefaultFanout = 12

  sealed trait Node[K, V, S, I] {
    type ThisType <: Node[K, V, S, I]
    val next: Option[I]
    def withNext(n: Option[I]): ThisType

    val summary: Option[S] = None
    val downOpt: Option[I] = None
    val keyOpt: Option[K] = None
    val valueOpt: Option[V] = None
  }

  sealed trait Branch[K, V, S, I] extends Node[K, V, S, I] {
    val down: I
    override val downOpt = Some(down)
    def invalidate: ThisType
    def withSummary(s: Option[S]): ThisType
  }

  case class LeafNode[K, V, S, I](key: K, value: V, next: Option[I]) extends Node[K, V, S, I] {
    type ThisType = LeafNode[K, V, S, I]
    def withNext(n: Option[I]) = LeafNode(key, value, n)

    override val valueOpt = Some(value)
    override val keyOpt = Some(key)
  }

  case class BranchNode[K, V, S, I](key: K, override val summary: Option[S], next: Option[I], down: I) extends Branch[K, V, S, I] {
    type ThisType = BranchNode[K, V, S, I]
    def withNext(n: Option[I]) = BranchNode(key, summary, n, down)
    def withSummary(s: Option[S]) = BranchNode(key, s, next, down)
    def invalidate = BranchNode[K, V, S, I](key, None, next, down)

    override val keyOpt = Some(key)
  }

  case class HeadBranchNode[K, V, S, I](next: Option[I], override val summary: Option[S], down: I) extends Branch[K, V, S, I] {
    type ThisType = HeadBranchNode[K, V, S, I]
    def withNext(n: Option[I]) = HeadBranchNode(n, summary, down)
    def withSummary(s: Option[S]) = HeadBranchNode(next, s, down)
    def invalidate = HeadBranchNode(next, None, down)
  }

  case class HeadLeafNode[K, V, S, I](next: Option[I]) extends Node[K, V, S, I] {
    type ThisType = HeadLeafNode[K, V, S, I]
    def withNext(n: Option[I]) = HeadLeafNode(n)
  }

  implicit def nodeSerializer[K: Serializer, V: Serializer, S: Serializer, I: Serializer] = {
    new MultiSerializer[Node[K, V, S, I]](
      (0, classOf[LeafNode[K,V,S,I]], caseClassSerializer(LeafNode.apply[K, V, S, I] _, LeafNode.unapply[K, V, S, I] _)),
      (1, classOf[BranchNode[K,V,S,I]], caseClassSerializer(BranchNode.apply[K, V, S, I] _, BranchNode.unapply[K, V, S, I] _)),
      (2, classOf[HeadBranchNode[K,V,S,I]], caseClassSerializer(HeadBranchNode.apply[K, V, S, I] _, HeadBranchNode.unapply[K, V, S, I] _)),
      (3, classOf[HeadLeafNode[K,V,S,I]], caseClassSerializer(HeadLeafNode.apply[K, V, S, I] _, HeadLeafNode.unapply[K, V, S, I] _))
    )
  }

  def sum[A](trav: Traversable[A])(implicit num: Numeric[A]): Option[A] = {
    trav.reduceOption[A]((x, y) => num.mkNumericOps(x) + y) // Stupid string implicits
  }
}

class MapReduceSkipList[K: Ordering, V, S, I]
    (reduce: Traversable[V] => Option[S],
     rereduce:  Traversable[S] => Option[S],
     rs: RecordSet[MapReduceSkipList.Node[K, V, S, I], I],
     fanout: Int = MapReduceSkipList.DefaultFanout) extends Hibernatable {
  import MapReduceSkipList._
  import Ordering.Implicits._

  private type Record = rs.RecordType
  private type Extent = rs.ExtentType
  private type Address = List[Record]

  // Since the constructor is a public method, constructor operations must be synced

  private var _lanes: List[Extent] = sync{List(rs())}
  private var _height = 1
  private def height = _height
  private def lanes = _lanes
  // Update height when adding lanes
  private def lanes_=(l: List[Extent]) = {
    _lanes = l
    _height = lanes.size
  }

  private val random = new Random()
  private var root: Record = sync{lanes(0)(HeadLeafNode(None))}
  private var tail: Address = List(root)

  private def lastK = tail.headOption flatMap {head => head().keyOpt}

  private def rollDice = random.nextInt(fanout) == 0

  private def selectHeight = {
    var newHeight = 1
    while (rollDice && newHeight < height) {
      newHeight += 1
    }
    newHeight
  }

  private def inTail(k: K) = {
    lastK match {
      case Some(lk) if k < lk =>
        // Not in tail, because it's less than the last k
        false
      case _ =>
        // In tail, either because the list is empty, or lastK is less than or equal to than k
        true
    }
  }

  private def beyondTail(k: K) = {
    lastK match {
      case Some(lk) if k <= lk =>
        // Not beyond tail, because it's no greater than the last k
        false
      case _ =>
        // Beyond tail, either because the list is empty, or lastK is strictly smaller than k
        true
    }
  }

  def +=(e: (K, V)) = sync {
    val (k, v) = e
    if (inTail(k)) {
      tail = insert(k, v, tail)
    } else {
      val address = greatestLowerBoundAddress(k)
      insert(k, v, address)
      tail = regenerateTail
    }
  }

  private def insert(k: K, v: V, address: Address): Address = {
    invalidateSummaries(address)
    val newHeight = selectHeight

    var down: Option[I] = None
    val patchedRecs = for ((lane, oldRecord) <- (lanes zip address).take(newHeight)) yield {
      val oldNode = oldRecord()
      val oldNext = oldNode.next
      val newNode: Node[K, V, S, I] = down match {
        case None =>
          require(!oldNode.isInstanceOf[Branch[K, V, S, I]])
          LeafNode(k, v, oldNext)
        case Some(idx) =>
          require(oldNode.isInstanceOf[Branch[K, V, S, I]])
          BranchNode(k, None, oldNext, idx)
      }
      val newRecord = lane(newNode)
      down = Some(newRecord.index)
      oldRecord() = oldNode withNext Some(newRecord.index)
      oldRecord.save
      newRecord
    }
    val newAddress = patchedRecs ++ address.drop(newHeight)
    if ((newHeight == lanes.size) && rollDice) addRow(k, newAddress) else newAddress
  }

  private def invalidateSummaries(address: Address) = {
    val victims = address.view map {record =>
      (record, record())
    } dropWhile { // Drop the lowest few - which are too small for summaries
      case (record, node) => node.summary.isEmpty
    } takeWhile { // take the remainder with summaries
      case (record, node) => node.summary.isDefined
    }

    for ((record, node) <- victims) {
      record() = node.asInstanceOf[Branch[K, V, S, I]].invalidate
      record.save
    }
  }

  private def addRow(k: K, address: Address): Address = {
    val lane = addLane()
    val lastRec = address.last
    val newRec = lane(BranchNode(k, None, None, lastRec.index))
    root() = root() withNext Some(newRec.index)
    root.save
    address :+ newRec
  }

  private def addLane() = {
    val newLane = rs()
    lanes :+= newLane
    val oldRootIndex = root.index
    root = newLane(HeadBranchNode(None, None, oldRootIndex))
    newLane
  }

  /**
   * Find last address that's less than or equal to k
   */
  private def greatestLowerBoundAddress(k: K) = {
    if (inTail(k)) tail
    else {
      keyBasedAddress(root, Nil)(_ <= k)
    }
  }
  /**
   * Find the last address that's strictly less than k
   */
  private def greatestStrictLowerBoundAddress(k: K) = {
    if (beyondTail(k)) tail
    else {
      keyBasedAddress(root, Nil)(_ < k)
    }

  }

  private def keyBasedAddress(currentRecord: Record, currentAddress: Address)(goNext: K => Boolean): Address = {
    generateAddress(currentRecord, currentAddress){(currentRecord, nextRec) =>
      nextRec().keyOpt match {
        case Some(nextKey) =>
          if (goNext(nextKey)) {
            true
          } else {
            false
          }
        case None => ??? // Next should never be a head node
      }
    }
  }

  //For debugging - delete when done
  def address(k: K): Any = sync(greatestLowerBoundAddress(k))

  /**
   * Find the last address
   */
  private def regenerateTail: Address = {
    generateAddress(root, Nil){(current, next) => true}
  }

  /**
   * Generate an address, based on a condition that says whether to go right (next) or down.
   *
   * Warning: If used improperly, the address generated may not be canonical. There may be
   * another address, to the right of this one, which reaches the same node. This may or may not be a problem
   * (for insertion, it's a killer, for querying it may not be). In any case, algorithms should prefer
   * going right to going down.
   */
  @tailrec private def generateAddress(currentRecord: Record, currentAddress: Address)(goNext: (Record, Record) => Boolean): Address = {
    val node = currentRecord()
    node match {
      case branch: Branch[K, V, S, I] =>
        val nextOpt = branch.next
        nextOpt match {
          case Some(next) =>
            val nextRec = rs(next)
            if (goNext(currentRecord, nextRec)) {
              generateAddress(nextRec, currentAddress)(goNext)
            } else {
              generateAddress(rs(branch.down), currentRecord :: currentAddress)(goNext)
            }
          case None =>
            generateAddress(rs(branch.down), currentRecord :: currentAddress)(goNext)
        }
      case leaf =>
        val next = leaf.next
        next match {
          case Some(next) =>
            val nextRec = rs(next)
            if (goNext(currentRecord, nextRec)) {
              generateAddress(nextRec, currentAddress)(goNext)
            } else {
              currentRecord :: currentAddress
            }
          case None => currentRecord :: currentAddress
        }
    }
  }

  def summary = sync {
    val roots = new SegmentTraverser(root, None)
    val sum = rereduce(roots.map(summarize).flatten)
    tail = regenerateTail // Tail can get out-of-sync with data
    sum
  }

  private def summarizeBranchSegment(start: Record, end: Option[I]) = {
    val summaries = (new SegmentTraverser(start, end) map {seqRec =>
      summarize(seqRec)
    }).flatten
    rereduce(summaries)
  }

  private def summarizeLeafSegment(start: Record, end: Option[I]) = {
    val values = (new SegmentTraverser(start, end) map {seqRec =>
      seqRec().valueOpt
    }).flatten
    reduce(values)
  }

  private def summarizeBranchNode(node: Branch[K, V, S, I]) = {
    val start = rs(node.down)
    val end = node.next flatMap (i => rs(i)().downOpt)
    start() match {
      case _: Branch[K, V, S, I] =>
        summarizeBranchSegment(start, end)
      case _ =>
        // Leaf nodes
        summarizeLeafSegment(start, end)
    }

  }

  private def summarize(rec: Record): Option[S] = {
    rec() match {
      case branch: Branch[K, V, S, I] =>
        branch.summary match {
          case Some(s) => branch.summary
          case None =>
            val sum = summarizeBranchNode(branch)
            rec() = branch withSummary sum
            rec.save
            sum
        }
      case LeafNode(key, value, next) =>
        reduce(Seq(value))
      case _ =>
        None
    }
  }

  def iterator: Iterator[(K, V)] = sync(new FullIterator)

  /*
   * Only sync public methods. Everything else will have been called via one,
   * so this allows optimisations, like tail call optimisation
   */
  override def sync[B](f: => B) = rs.sync(f)
  def hibernate() = rs.hibernate()

  private class SegmentTraverser(start: Record, End: Option[I]) extends Traversable[Record] {
    override def foreach[U](f: Record => U): Unit = sync {
      process(start, f)
    }
    @tailrec private def process[U](rec: Record, f: Record => U): Unit = {
      f(rec)
      val node = rec()
      node.next match {
        case End => ()
        case None => ()
        case Some(next) => process(rs(next), f)
      }
    }
  }

  private class FullIterator extends Iterator[(K, V)] {
    private var record = findBottom(root)
    private def findBottom(from: Record): Record = {
      from().downOpt match {
        case Some(down) =>
          findBottom(rs(down))
        case None =>
          from
      }
    }

    def hasNext = sync {
      record().next.isDefined
    }

    def next = sync {
      record = rs(record().next.get)
      val node = record().asInstanceOf[LeafNode[K, V, S, I]]
      (node.key, node.value)
    }
  }
}
