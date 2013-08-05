package uk.me.jamespic.dougng.util

object MapReduce {
  def sum[A](trav: Traversable[A])(implicit num: Numeric[A]): Option[A] = {
    trav.reduceOption[A]((x, y) => num.mkNumericOps(x) + y) // Stupid string implicits
  }
}

object MapReduceAlgorithm {
  import MapReduce._
  implicit def sumAlgo[A: Numeric] = MapReduceAlgorithm(sum[A] _, sum[A] _)
  implicit def combineAlgo[D, T <: Combinable[D, T]](implicit start: Start[D, T] with T) = MapReduceAlgorithm(
      mapReduce = {l: Traversable[D] =>
        Some(((start: T) /: l){_ + _})
      },
      reReduce = {l: Traversable[T] =>
      	Some(((start: T) /: l){_ ++ _})
      }
  )

  def memory[K: Ordering, V, S](implicit algo: MapReduceAlgorithm[V, S]) = {
    MapReduceAATree.empty[K, V, S](algo.mapReduce, algo.reReduce)
  }

  def disk[K: Ordering : Serializer, V: Serializer, S: Serializer](implicit algo: MapReduceAlgorithm[V, S], alloc: Allocator) = {
    new MapReduceBRTree[K, V, S](algo.mapReduce, algo.reReduce)(alloc)
  }
}

case class MapReduceAlgorithm[V, S](mapReduce: Traversable[V] => Option[S],
                                       reReduce: Traversable[S] => Option[S])

trait MapReduce[K, V, S] extends Traversable[(K, V)] {
  implicit def ordering: Ordering[K]
  def summary: Option[S] = summaryBetween(None, None)
  def summaryBetween(low: K, high: K): Option[S] = summaryBetween(Some(low), Some(high))
  def summaryBetween(low: Option[K], high: Option[K]): Option[S]
  def getBetween(low: Option[K], high: Option[K]): Traversable[(K, V)] = {
      new Traversable[(K, V)] {
        def foreach[U](f: ((K, V)) => U) = {
          doBetween(low, high, f)
        }
        override def stringPrefix = "View"
      }
    }
  def getBetween(low: K, high: K):  Traversable[(K, V)] = getBetween(Some(low), Some(high))
  def get(k: K) = getBetween(k, k)
  def minKey: Option[K]
  def maxKey: Option[K]
  def doBetween[U](low: Option[K], high: Option[K], f: ((K, V)) => U): Unit
  def foreach[U](f: ((K, V)) => U): Unit = doBetween(None, None, f)

  override def stringPrefix = "MapReduce"

}

trait MutableMapReduce[K, V, S] extends MapReduce[K, V, S] {
  def close: Unit
  def +=(e: (K, V)): Unit
  def ++=(entries: TraversableOnce[(K, V)]) = {
    for (e <- entries) this += e
  }
}

trait ImmutableMapReduce[K, V, S] extends MapReduce[K, V, S] {
  type SelfType <: ImmutableMapReduce[K, V, S]
  def +(e: (K, V)): SelfType
}

class MapReduceWrapper[K, V, S](private var imm: ImmutableMapReduce[K, V, S])
    extends MutableMapReduce[K, V, S] {
  def summaryBetween(low: Option[K], high: Option[K]) = synchronized {
    imm.summaryBetween(low, high)
  }
  def minKey = synchronized {imm.minKey}
  def maxKey = synchronized {imm.maxKey}
  def doBetween[U](low: Option[K], high: Option[K], f: ((K, V)) => U) = synchronized {
    imm.doBetween(low, high, f)
  }
  def close = ()
  def +=(e: (K, V)) = synchronized {imm += e}
  implicit def ordering = imm.ordering
}