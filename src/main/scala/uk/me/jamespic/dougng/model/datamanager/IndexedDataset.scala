package uk.me.jamespic.dougng.model.datamanager

import uk.me.jamespic.dougng.util.{MapReduceQuickSort, Stats}

class IndexedDataset(data: Map[String, Seq[(Long, Double)]]) {
  val datasets = for ((name, points) <- data) yield {
    name -> new MapReduceQuickSort[Long, Double, Stats](points, Stats.single _, _ + _)
  }
}