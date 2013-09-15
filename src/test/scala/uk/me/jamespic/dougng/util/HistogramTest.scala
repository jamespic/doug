package uk.me.jamespic.dougng.util

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen

class HistogramTest extends FunSpec with ShouldMatchers with GivenWhenThen {
  describe("A Histogram") {
    it("should be constructible and combinable") {
      val histograms = for (i <- 0 to 1000) yield {
        val data = for (j <- 0 to 1000) yield (i + j).toDouble
        val histogram = Histogram(data)
        val count = histogram.nonZero.map(_._2).sum
        count should equal(1001)
        histogram
      }
      val instance = time {Histogram.merge(histograms)}
      val count = instance.nonZero.map(_._2).sum
      count should equal(1001 * 1001)
      instance.nonZero.size should be >= 32
      instance.nonZero.last._1.from.toDouble should be <= 1001.0 * 1001.0
      instance.nonZero.head._1.to.toDouble should be >= 0.0
    }
    it("should work in a MapReduce setting") {
      implicit val alloc = Allocator()
      var instance = MapReduceAlgorithm.memory[Long, Double, Histogram]
      for (i <- 1 to 100) instance += i.toLong -> i.toDouble
      val summary = instance.summary.get
      summary.nonZero.size should be >= 32
      summary.nonZero.last._1.from.toDouble should be <= 100.0
      summary.nonZero.head._1.to.toDouble should be >= 0.0
    }
  }
}