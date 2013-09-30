package uk.me.jamespic.dougng.model.datamanager

import uk.me.jamespic.dougng.util.MutableMapReduce
import uk.me.jamespic.dougng.util.DetailedStats

trait DataStore {
  type Data = MutableMapReduce[Long, Double, DetailedStats]
  def apply(rowName: String): Data
  def toMap: Map[String, Data]
  def close: Unit
  def min: Long
  def max: Long
  def rows: Seq[String]
}