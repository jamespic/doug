package uk.me.jamespic.dougng.util

import shapeless._
import scala.collection.mutable.ArrayBuffer
import java.util.Random

object MapReduceSkipList {
  val DefaultFanout = 32

  case class LeafNode[K, V](key: K, value: V, next: Option[Long])
  case class BranchNode[K, S](key: K, summary: S, next: Option[Long], down: Long)
  type Node[K, V, S] = Either[LeafNode[K, V], BranchNode[K, S]]

  object LeafNode {
    implicit def serializer[K: Serializer, V: Serializer] = Serializer.caseClassSerializer(LeafNode.apply[K, V] _, LeafNode.unapply[K, V] _)
  }
  object BranchNode {
    implicit def serializer[K: Serializer, S: Serializer] = Serializer.caseClassSerializer(BranchNode.apply[K, S] _, BranchNode.unapply[K, S] _)
  }
}
/*
object MapReduceSkipList {
  val DefaultFanout = 32

  sealed trait Node[K, Next] {
    def key: K
    def next: Option[Next]
    def next_=(next: Option[Next]): Unit
  }

  final class LeafNode[K, V, Next](val key: K, val value: V, var next: Option[Next]) extends Node[K, Next] {
    override def toString = s"LeafNode($key, $value, $next)"
  }
  object LeafNode {
    def iso[K, V, Next] = new Iso[LeafNode[K, V, Next], K :: V :: Option[Next] :: HNil] {
      def to(node: LeafNode[K, V, Next]) = node.key :: node.value :: node.next :: HNil
      def from(hlist: K :: V :: Option[Next] :: HNil) = hlist match {
       case key :: value :: next :: HNil => new LeafNode(key, value, next)
      }
    }
    implicit def serializer[K: Serializer, V: Serializer, Next: Serializer] = {
      Serializer.isoSerializer(iso[K, V, Next])
    }
    **
     * Helper type, to allow Typer to be defined
     *
    type Wrapper[K, V] = {
      type Type[Next] = LeafNode[K, V, Next]
    }
    type Typer[T[_], K, V] = RecursiveTyper[T, MapReduceSkipList.LeafNode.Wrapper[K, V]#Type]
    class RecursiveSerializer[K:Serializer, V: Serializer] extends RecursiveTyper[Serializer, Wrapper[K, V]#Type] {
      def apply[B](s: Serializer[B]) = {
        implicit val serb = s
        implicitly[Serializer[LeafNode[K, V, B]]]
      }
    }
    implicit def recSerializer[K: Serializer, V: Serializer] = new RecursiveSerializer[K, V]
    implicit def recDummyTyper[K, V] = Dummy.dummyTyper[MapReduceSkipList.LeafNode.Wrapper[K, V]#Type]
  }

  final class BranchNode[K, S, Next, Down](val key: K, var value: Option[S], var next: Option[Next], var down: Down) extends Node[K, Next] {
    override def toString = s"BranchNode($key, $value, $next, $down)"
  }
  object BranchNode {
    def iso[K, S, Next, Down] = new Iso[BranchNode[K, S, Next, Down], K :: Option[S] :: Option[Next] :: Down :: HNil] {
      def to(node: BranchNode[K, S, Next, Down]) = node.key :: node.value :: node.next :: node.down :: HNil
      def from(hlist: K :: Option[S] :: Option[Next] :: Down :: HNil) = hlist match {
       case key :: value :: next :: down :: HNil => new BranchNode(key, value, next, down)
      }
    }
    implicit def serializer[K: Serializer, S: Serializer, Next: Serializer, Down: Serializer] = {
      Serializer.isoSerializer(iso[K, S, Next, Down])
    }
    **
     * Helper type, to allow Typer to be defined
     *
    type Wrapper[K, S, Down] = {
      type Type[Next] = BranchNode[K, S, Down, Next]
    }
    class RecursiveSerializer[K: Serializer, S: Serializer, Down: Serializer] extends RecursiveTyper[Serializer, Wrapper[K, S, Down]#Type] {
      def apply[B](s: Serializer[B]) = {
        implicit val serb = s
        implicitly[Serializer[BranchNode[K, S, Down, B]]]
      }
    }
    type TWrapper[T[_], K, S] = {
      type Type[Down] = RecursiveTyper[T, Wrapper[K, S, Down]#Type]
    }
    type TwoWayTyper[T[_], K, S] = T ~> TWrapper[T, K, S]#Type
    class TwoWaySerializer[K: Serializer, S: Serializer] extends (Serializer ~> TWrapper[Serializer, K, S]#Type) {
      def apply[Down](s: Serializer[Down]) = {
        implicit val serd = s
        new RecursiveSerializer[K, S, Down]
      }
    }
    implicit def twoWaySerializer[K: Serializer, S: Serializer] = new TwoWaySerializer[K, S]
    class TwoWayDummyTyper[K, S] extends (Dummy ~> TWrapper[Dummy, K, S]#Type) {
      def apply[Down](dummy: Dummy[Down]) = new Dummy.DummyTyper[Wrapper[K, S, Down]#Type]
    }
    implicit def twoWayDummyTyper[K, S] = new TwoWayDummyTyper[K, S]
  }
}

class MapReduceSkipList[T[_], F <: RecordUniverse[T], K: T, V: T, S: T](
    reduce: Traversable[V] => S,
    rereduce: Traversable[S] => S,
    universe: F,
    fanout: Int = MapReduceSkipList.DefaultFanout)
    (implicit ordering: Ordering[K],
     leafTyper: MapReduceSkipList.LeafNode.Typer[T, K, V],
     branchTyper: MapReduceSkipList.BranchNode.TwoWayTyper[T, K, S])
    extends Hibernatable {
  import MapReduceSkipList._
  import Ordering.Implicits._

  private type Record[A] = universe.RecordSetType[A]#Record
  private type Index[A] = universe.RecordSetType[A]#Index

   *
   * Addresses are lists of records (containing nodes). Leaf nodes are first,
   * with root nodes last.
   *
  private type Address = List[Record[_]]

  private var random = new Random(0)
  private var head: Option[Record[_]] = None
  private var tail: List[Record[_]] = Nil
  private var lanes: List[universe.RecordSetType[_]] = {
    List(universe.recursive[LeafNode.Wrapper[K, V]#Type](leafTyper))
  }

  override def hibernate() = universe.hibernate()
  override def sync[B](f: => B) = universe.sync(f)

  private def firstLane = universe.sync {
    val lane = lanes.head
    lane.asInstanceOf[universe.RecordSetType[LeafNode[K, V, lane.IndexType]]]
  }

  def +=(e: (K, V)) = universe.sync {
    val (k, v) = e
    head match {
      case None =>
        val rec = firstLane.addRec(new LeafNode(k, v, None))
        head = Some(rec)
        tail = List(rec)
        ()
      case Some(node) =>
        val lastK = tail.last().asInstanceOf[Node[K, _]].key
        if (k >= lastK) {
          // short circuit, for already-ordered data
          tail = insert(k, v, tail)
        } else {
          ???
        }
        ()
    }
  }

  private def rollDice = random.nextInt(fanout) == 0

  private def selectHeight = {
    var newHeight = 1
    val currentHeight = lanes.size
    while (rollDice && newHeight < currentHeight) {
      newHeight += 1
    }
    newHeight
  }

  private def insert(k: K, v: V, address: Address): Address = universe.sync {
    invalidateSummaries(address)
    var newHeight = selectHeight


    var lastRecordCreated: Option[Record[_]] = None
    val patchedRecs = for ((lane, oldRecord) <- (lanes zip address).take(newHeight)) yield {
      val oldNode = oldRecord().asInstanceOf[Node[K, lane.IndexType]]
      val oldNext = oldNode.next
      val newNode = lastRecordCreated match {
        case None => new LeafNode(k, v, oldNext)
        case Some(rec) => new BranchNode(k, None, oldNext, rec.index)
      }
      val newRecord = lane.asInstanceOf[universe.RecordSetType[Any]].addRec(newNode)
      lastRecordCreated = Some(newRecord)
      oldNode.next = Some(newRecord.index.asInstanceOf[lane.IndexType])
      oldRecord.save
      newRecord
    }
    val newAddress = patchedRecs ++ address.drop(newHeight)
    if ((newHeight == lanes.size) && rollDice) addRow(k, newAddress) else newAddress
  }

  private def addRow(k: K, address: Address): Address = {
    val lane = addLane.asInstanceOf[universe.RecordSetType[Any]]
    val lastRec = address.last
    if (head.get.index == lastRec.index) {
      val newHeadNode = new BranchNode(k, None, None, lastRec.index)
      val newRec = lane.addRec(newHeadNode)
      head = Some(newRec)
      address :+ newRec
    } else {
      val newHeadNode = new BranchNode(k, None, (None: Option[lane.IndexType]), head.get.index)
      val newHeadRec = lane.addRec(newHeadNode)
      val newAddressNode = new BranchNode(k, None, None, lastRec.index)
      val newAddressRec = lane.addRec(newAddressNode)
      newHeadNode.next = Some(newAddressRec.index)
      newHeadRec.save
      head = Some(newHeadRec)
      address :+ newAddressRec
    }
  }

  private def addLane() = {
    val oldLastLane = lanes.last
    val typer = branchTyper.apply(oldLastLane.reifyIndex)
    val newLane = universe.recursive[BranchNode.Wrapper[K, S, oldLastLane.IndexType]#Type](typer)
    lanes :+= newLane
    newLane
  }

  private def invalidateSummaries(address: Address) = {
    for (record <- address; node = record();
         if node.isInstanceOf[BranchNode[K, S, _, _]];
         typedNode = node.asInstanceOf[BranchNode[K, S, _, _]];
         if typedNode.value.isDefined
         ) {
      typedNode.value = None
      record.save
    }
  }

  private def findAddress(k: K) = {
    val lastK = tail.head().asInstanceOf[Node[K, _]].key
    if (k >= lastK) tail
    else {
      partialAddress(k, head.get, Nil)
    }
  }

  //For debugging - delete when done
  def address(k: K): Any = universe.sync(findAddress(k))

  private def partialAddress(k: K, currentNode: Record[_], currentAddress: Address): Address = {
    val node = currentNode()
    node match {
      case leaf: LeafNode[K, V, _] =>
        val next = leaf.next
        next match {
          case Some(next: Index[LeafNode[K, V, _]]) if next.get().key < k =>
            partialAddress(k, next.get, currentAddress)
          case _ => currentNode :: currentAddress
        }
      case branch: BranchNode[K, S, _, _] =>
        val nextOpt = branch.next
        println(branch)
        nextOpt.foreach(x => println(x.asInstanceOf[Index[_]].get))
        nextOpt match {
          case Some(next: Index[BranchNode[K, S, _, _]]) if next.get().key < k =>
            partialAddress(k, next.get, currentAddress)
          case _ =>
            val down = branch.down.asInstanceOf[Index[_]]
            partialAddress(k, down.get.asInstanceOf[Record[_]], currentNode :: currentAddress)
        }
    }
  }
}
*/
