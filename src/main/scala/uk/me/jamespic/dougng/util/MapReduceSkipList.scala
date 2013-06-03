package uk.me.jamespic.dougng.util

import shapeless._
import scala.collection.mutable.ArrayBuffer
import java.util.Random

object MapReduceSkipList {
  import Serializer.{caseClassSerializer, MultiSerializer}
  val DefaultFanout = 32

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
    def invalidate = BranchNode[K, V, S, I](key, None, next, down)

    override val keyOpt = Some(key)
  }

  case class HeadBranchNode[K, V, S, I](next: Option[I], override val summary: Option[S], down: I) extends Branch[K, V, S, I] {
    type ThisType = HeadBranchNode[K, V, S, I]
    def withNext(n: Option[I]) = HeadBranchNode(n, summary, down)
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
}

class MapReduceSkipList[K: Ordering, V, S, I]
    (reduce: Traversable[V] => S,
     rereduce:  Traversable[S] => S,
     rs: RecordSet[MapReduceSkipList.Node[K, V, S, I], I],
     fanout: Int = MapReduceSkipList.DefaultFanout) extends Hibernatable {
  import MapReduceSkipList._
  import Ordering.Implicits._

  private type Record = rs.RecordType
  private type Extent = rs.ExtentType
  private type Address = List[Record]

  // Update height when adding lanes
  private var _lanes: List[Extent] = List(rs())
  private var _height = 1
  private def height = _height
  private def lanes = _lanes
  private def lanes_=(l: List[Extent]) = sync {
    _lanes = l
    _height = lanes.size
  }

  private val random = new Random()
  private var root: Record = lanes(0)(HeadLeafNode(None))
  private var tail: Address = List(root)

  private def lastK = tail.headOption flatMap {head => head().keyOpt}

  private def rollDice = random.nextInt(fanout) == 0

  private def selectHeight = sync {
    var newHeight = 1
    while (rollDice && newHeight < height) {
      newHeight += 1
    }
    newHeight
  }

  private def inTail(k: K) = sync {
    lastK match {
      case Some(lk) if k < lk =>
        // Not in tail, because it's not greater than head
        false
      case _ =>
        // In tail, because head is either empty, or smaller than k
        true
    }
  }

  def +=(e: (K, V)) = sync {
    val (k, v) = e
    if (inTail(k)) {
      tail = insert(k, v, tail)
    } else {
      val address = findAddress(k)
      insert(k, v, address)
      tail = regenerateTail()
    }
  }

  private def insert(k: K, v: V, address: Address): Address = sync {
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

  private def invalidateSummaries(address: Address) = sync {
    for {record <- address
      node = record()
      if node.summary.isDefined
      } {
        record() = node.asInstanceOf[Branch[K, V, S, I]].invalidate
        record.save
      }
  }

  private def addRow(k: K, address: Address): Address = sync {
    val lane = addLane()
    val lastRec = address.last
    val newRec = lane(BranchNode(k, None, None, lastRec.index))
    root() = root() withNext Some(newRec.index)
    root.save
    address :+ newRec
  }

  private def addLane() = sync {
    val newLane = rs()
    lanes :+= newLane
    val oldRootIndex = root.index
    root = newLane(HeadBranchNode(None, None, oldRootIndex))
    newLane
  }


  private def findAddress(k: K) = sync {
    if (inTail(k)) tail
    else {
      partialAddress(k, root, Nil)
    }
  }

  //For debugging - delete when done
  def address(k: K): Any = sync(findAddress(k))

  private def partialAddress(k: K, currentRecord: Record, currentAddress: Address): Address = sync {
    val node = currentRecord()
    node match {
      case branch: Branch[K, V, S, I] =>
        val nextOpt = branch.next
        nextOpt match {
          case Some(next) =>
            val nextRec = rs(next)
            nextRec().keyOpt match {
              case Some(nextKey) => if (nextKey <= k) {
                  partialAddress(k, nextRec, currentAddress)
                } else {
                  partialAddress(k, rs(branch.down), currentRecord :: currentAddress)
                }
              case None => ??? // Next should never be a head node
            }
          case None =>
            partialAddress(k, rs(branch.down), currentRecord :: currentAddress)
        }
      case leaf =>
        val next = leaf.next
        next match {
          case Some(next) =>
            val nextRec = rs(next)
            nextRec().keyOpt match {
              case Some(nextKey) => if (nextKey <= k) {
                  partialAddress(k, nextRec, currentAddress)
                } else {
                  currentRecord :: currentAddress
                }
              case None => ??? // Next should never be a head node
            }
          case None => currentRecord :: currentAddress
        }
    }
  }

  private def regenerateTail(currentRecord: Record = root, currentAddress: Address = Nil): Address = sync {
    currentRecord() match {
      case branch: Branch[K, V, S, I] =>
        branch.next match {
          case Some(next) => regenerateTail(rs(next), currentAddress)
          case None => regenerateTail(rs(branch.down), currentRecord :: currentAddress)
        }
      case leaf =>
        leaf.next match {
          case Some(next) => regenerateTail(rs(next), currentAddress)
          case None => currentRecord :: currentAddress
        }
    }
  }

  def iterator: Iterator[(K, V)] = new FullIterator

  override def sync[B](f: => B) = rs.sync(f)
  def hibernate() = rs.hibernate()

  private class FullIterator extends Iterator[(K, V)] {
    private var record = sync(findBottom(root))
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
