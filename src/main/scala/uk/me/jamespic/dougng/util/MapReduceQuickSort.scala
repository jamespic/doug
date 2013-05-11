package uk.me.jamespic.dougng.util

import shapeless._
import Ordering.Implicits.infixOrderingOps
import scala.collection.mutable.ArrayBuilder
import scala.collection.mutable.ArrayBuffer

class MapReduceQuickSort[K, V, S](input: IndexedSeq[(K, V)], map: V => S, reduce: (S, S) => S, fanout: Int = 256)
    (implicit serk: Serializer[K], serv: Serializer[V], sers: Serializer[S], ord: Ordering[K]){
  require ((fanout & fanout - 1) == 0, "fanout must be a power of 2")

  // FIXME: Implicits are stubborn
  private def dlb[A: Serializer, B: Serializer] = new DiskListBuilder[(A, B)]
  private def treeSerDef[K1: Serializer, V1: Serializer, S1: Serializer] = {
    Serializer.caseClassSerializer(TreeNode.apply[K1, S1] _, TreeNode.unapply[K1, S1] _)
  }

  implicit val treeSer = treeSerDef[K, V, S]

  // Make this private, once you're done poking around
  val (rootNode, list, tree) = {
    val listBuilder = dlb[K, V]
    val treeBuilder = new DiskListBuilder[TreeNode[K, S]]
    var listIndex = 0
    var treeIndex = 0

    def partition(data: IndexedSeq[(K, V)]) = {
      val pivots = (for (i <- 1L to fanout.toLong) yield {
        data((i * (data.size - 1) / (fanout + 1)).toInt)._1
      }).sorted

      def bucket(key: K, low: Int = 0, size: Int = fanout): Int = {
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

      val buckets = for (i <- 0 until fanout) yield {
        if (data.size > 65536) {
          //System.gc()
          dlb[K, V]
        } else {
          new ArrayBuffer[(K, V)]
        }
      }

      for ((k, v) <- data) {
        buckets(bucket(k)) += k -> v
      }

      buckets.map(_.result())
    }

    def sort(part: IndexedSeq[(K, V)]): TreeNode[K, S] = {
      if (part.size > fanout) {
        val subParts = partition(part)
        val nodes = for (subPart <- subParts) yield sort(subPart)
        val node = nodes.view reduce {(n1, n2) =>
          TreeNode(
              reduce(n1.summary, n2.summary),
              n1.lowKey min n2.lowKey,
              n1.highKey min n2.highKey,
              n1.firstRec,
              n1.recs + n2.recs,
              Some(treeIndex), Some(nodes.size)
          )
        }
        treeBuilder ++= nodes
        treeIndex += nodes.size
        node
      } else {
        val sorted = part.toArray.sortBy(_._1)
        val (summary, min, max) = part.view map {
          case (k, v) => (map(v), k, k)
        } reduce { (x, y) =>
          val (s1, min1, max1) = x
          val (s2, min2, max2) = y
          (reduce(s1, s2), min1 min min2, max2 max max2)
        }
        listBuilder ++= sorted
        val node = TreeNode(summary, min, max, listIndex, sorted.size, None, None)
        listIndex += sorted.size
        node
      }
    }

    (sort(input), listBuilder.result(),treeBuilder.result())
  }


}

private[util] case class TreeNode[K: Serializer, S: Serializer](summary: S,
      lowKey: K, highKey: K,
      firstRec: Int, recs: Int,
      firstChild: Option[Int], children: Option[Int])