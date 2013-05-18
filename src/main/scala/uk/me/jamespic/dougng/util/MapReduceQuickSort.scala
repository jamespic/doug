package uk.me.jamespic.dougng.util

import shapeless._
import Ordering.Implicits.infixOrderingOps
import scala.collection.mutable.ArrayBuilder
import scala.collection.mutable.ArrayBuffer
import scala.collection.SeqView
import scala.collection.SeqLike
import scala.collection.generic.CanBuildFrom

object MapReduceQuickSort {
  val InMemorySize = 4096
  val DefaultFanout = 32
}

class MapReduceQuickSort[K, V, S](input: IndexedSeq[(K, V)],
    map: V => S,
    reduce: (S, S) => S,
    fanout: Int = MapReduceQuickSort.DefaultFanout)
    (implicit serk: Serializer[K], serv: Serializer[V], sers: Serializer[S], val ordering: Ordering[K]) {
  require ((fanout & fanout - 1) == 0, "fanout must be a power of 2")

  /*
   *  The initialisation is all done in a closure. We use a decorate-sort-undecorate
   *  method to make this a stable sort - this is necessary, as our algorithm doesn't
   *  handle duplicate keys.
   */
  private val (rootNode, list, tree) = {
    // Some types, to save on typing
    type KI = (K, Int)
    type ISV[K, V] = SeqLike[(KI, V), IndexedSeq[(KI, V)]]

    /*
     *  FIXME: We pre-chew some complicated implicit conversions, as it tends to
     *  complain about diverging implicits otherwise. I'd really like to figure out why this is
     */
    implicit val sliceSer = Serializer.caseClassSerializer(Slice.apply _, Slice.unapply _)
    def dlb[A: Serializer, B: Serializer] = new DiskListBuilder[(A, B)]
    def treeSerDef[K1: Serializer, S1: Serializer] = {
      Serializer.caseClassSerializer(TreeNode.apply[K1, S1] _, TreeNode.unapply[K1, S1] _)
    }
    def kvCbf[K1: Serializer, V1: Serializer] = {
      DiskList.canBuildFrom[((K1, Int), V1)]
    }

    implicit val treeSer = treeSerDef[K, S]
    implicit val cbf = kvCbf[K, V]


    val listBuilder = dlb[K, V]
    val treeBuilder = new DiskListBuilder[TreeNode[K, S]]
    var listIndex = 0
    var treeIndex = 0

    // Partition a data set into a series of sub-partitions
    def partition(data: ISV[K, V]) = {
      val pivots = (for (i <- 1L to fanout.toLong) yield {
        data((i * (data.size - 1) / (fanout + 1)).toInt)._1
      }).sorted

      /**
       * Binary search to decide which bucket a key belongs in
       */
      def bucket(key: KI, low: Int = 0, size: Int = fanout): Int = {
        if (size == 1) {
          low
        } else {
          val mid = low + size / 2
          if (key < pivots(mid)) {
            bucket(key, low, size / 2)
          } else {
            bucket(key, low + size / 2, size / 2)
          }
        }
      }

      /**
       * Bucket generator - below a certain size, allocate buckets in memory
       */
      val buckets = for (i <- 0 until fanout) yield {
        if (data.size > MapReduceQuickSort.InMemorySize) {
          dlb[KI, V]
        } else {
          new ArrayBuffer[(KI, V)]
        }
      }

      // Put values in appropriate buckets
      for ((k, v) <- data) buckets(bucket(k)) += (k -> v)

      buckets.map(_.result())
    }

    /*
     * The sort algorithm. Partition, then recursively a set. Additionally, we
     * create the tree as we go. If the size of the partition is below the fanout of
     * the data structure, it becomes a leaf node.
     */
    def sort(part: ISV[K, V]): TreeNode[K, S] = {
      try {
        if (part.size > fanout) {
          // Branch node
          val subParts = partition(part)
          val nodes = for (subPart <- subParts) yield sort(subPart)
          val node = nodes.view reduce {(n1, n2) =>
            TreeNode(
                reduce(n1.summary, n2.summary),
                n1.lowKey min n2.lowKey,
                n1.highKey max n2.highKey,
                Slice(n1.recs.start, n1.recs.length + n2.recs.length),
                Some(Slice(treeIndex, nodes.size))
            )
          }
          treeBuilder ++= nodes
          treeIndex += nodes.size
          node
        } else {
          // Leaf node
          val sorted = part.toArray.sortBy(_._1).map {case ((k, i),v) => (k, v)}
          val (summary, min, max) = part.view map {
            case (k, v) => (map(v), k._1, k._1)
          } reduce { (x, y) =>
            val (s1, min1, max1) = x
            val (s2, min2, max2) = y
            (reduce(s1, s2), min1 min min2, max2 max max2)
          }
          listBuilder ++= sorted
          val node = TreeNode(summary, min, max, Slice(listIndex, sorted.size), None)
          listIndex += sorted.size
          node
        }
      } finally {
        // Part is never used again, so we free it
        if (part.isInstanceOf[DiskList[_]]) part.asInstanceOf[DiskList[_]].close()
      }
    }
    // Add indexes to data
    val indexedData = input.view.zipWithIndex.map{case ((k, v), i) => ((k, i),v)}.to[DiskList]
    (sort(indexedData),
        listBuilder.result(),treeBuilder.result())
  }

  // Methods start here

  def iterator = list.iterator
  def get(k:K) = getBetweenNode(k, k, rootNode)
  def getBetween(low: K, high: K) = getBetweenNode(low, high, rootNode)
  def maxKey = rootNode.highKey
  def minKey = rootNode.lowKey

  /*
   * FIXME: There's some code duplication between the get and summary code - refactor it into a general
   * map-reduce type thing?
   */
  private def getBetweenNode(low: K, high: K, node: TreeNode[K, S]): Seq[V] = {
    if (high < node.lowKey || low > node.highKey) {
      Nil
    } else if (low <= node.lowKey && high >= node.highKey) {
      list.view.slice(node.recs.start, node.recs.start + node.recs.length) map (_._2)
    } else {
      node.children match {
        case Some(Slice(child, children)) =>
          val childNodes = tree.view.slice(child, child + children)
          childNodes.flatMap({cn => getBetweenNode(low, high, cn)})
        case None =>
          for ((ki, v) <- list.view.slice(node.recs.start, node.recs.start + node.recs.length)
              if (ki >= low) && (ki <= high)) yield v
      }
    }.toSeq
  }

  def summaryBetween(low: K, high: K) = summaryBetweenNode(low, high, rootNode)
  private def summaryBetweenNode(low: K, high: K, node: TreeNode[K, S]): Option[S] = {
    if (high < node.lowKey || low > node.highKey) {
      None
    } else if (low <= node.lowKey && high >= node.highKey) {
      Some(node.summary)
    } else {
      node.children match {
        case Some(Slice(child, children)) =>
          val childNodes = tree.view.slice(child, child + children)
          childNodes flatMap {cn => summaryBetweenNode(low, high, cn)} reduceOption reduce
        case None =>
          val values = for ((ki, v) <- list.view.slice(node.recs.start, node.recs.start + node.recs.length).view
              if (ki >= low) && (ki <= high)) yield v
          values map map reduceOption reduce
      }
    }
  }

  // FIXME: Test this
  def leastUpperBound(k: K): Option[K] = leastUpperBound(k, rootNode)
  private def leastUpperBound(k: K, node: TreeNode[K, S]): Option[K] = {
    if (k < node.lowKey || k > node.highKey) {
      None
    } else {
      node.children match {
        case Some(Slice(child, children)) =>
          val childNodes = tree.view.slice(child, child + children)
          childNodes.flatMap(leastUpperBound(k, _)).find(k <= _)
        case None =>
          val entries = list.view.slice(node.recs.start, node.recs.start + node.recs.length)
          entries.collectFirst {case (ek, ev) if k <= ek => ek}
      }
    }
  }

}

private[util] case class Slice(start: Int, length: Int)

private[util] case class TreeNode[K: Serializer, S: Serializer](summary: S,
      lowKey: K, highKey: K,
      recs: Slice,
      children: Option[Slice])