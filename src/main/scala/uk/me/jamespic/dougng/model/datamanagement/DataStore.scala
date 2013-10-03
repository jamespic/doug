package uk.me.jamespic.dougng.model.datamanagement

import uk.me.jamespic.dougng.util.MutableMapReduce
import uk.me.jamespic.dougng.util.DetailedStats
import uk.me.jamespic.dougng.util.MapReduce
import uk.me.jamespic.dougng.util.MapReduceWrapper
import uk.me.jamespic.dougng.util.Allocator

trait DataStore {
  type Data = MutableMapReduce[Long, Double, DetailedStats]
  def toMap: Map[String, Data]
  def apply(rowName: String): Data
  def close: Unit = toMap.values.foreach(_.close)
  def min = (Long.MaxValue /: toMap.values.flatMap(_.minKey))(_ min _)
  def max = (Long.MinValue /:toMap.values.flatMap(_.maxKey))(_ max _)
  def rows = toMap.keySet
}

abstract class FactoryDataStore extends DataStore {
  protected def construct: Data
  private var map = Map.empty[String, Data]
  def toMap = map
  def apply(rowName: String) = {
    map.orElse[String, Data]{case key =>
      val data = construct
      map += key -> data
      data
    } apply rowName
  }
}

object DataStore {
  def memory = new FactoryDataStore {
    def construct = new MapReduceWrapper(MapReduce.memory)
  }
  def disk = new FactoryDataStore {
    private implicit val alloc = Allocator()
    def construct = MapReduce.disk
    override def close = {
      super.close
      alloc.close
    }
  }
}