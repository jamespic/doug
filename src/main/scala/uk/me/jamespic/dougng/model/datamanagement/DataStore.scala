package uk.me.jamespic.dougng.model.datamanagement

import shapeless._
import uk.me.jamespic.dougng.util._

trait DataStore {
  type Data = MutableMapReduce[Long, Double, Stats]
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
    def construct = new MapReduceWrapper(MapReduce.memory[Long, Double, Stats])
  }
  def disk = new FactoryDataStore {
    private implicit val alloc = Allocator()
    implicitly[Serializer[Stats]]
    def construct = MapReduce.disk[Long, Double, Stats]
    override def close = {
      super.close
      alloc.close
    }
  }
}