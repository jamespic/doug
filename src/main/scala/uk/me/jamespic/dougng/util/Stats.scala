package uk.me.jamespic.dougng.util

import org.apache.commons.math3.stat.descriptive.StatisticalSummary
import shapeless.Generic

import scala.math._


case class Stats(sum: Double, count: Long, max: Double, min: Double, sumSq: Double, histogram: Histogram) extends StatisticalSummary {
  def getMax: Double = max
  def getMean: Double = if (count > 0) {sum / count} else {0}
  def getMin: Double = min
  def getN: Long = count
  def getStandardDeviation: Double = sqrt(getVariance())
  def getSum: Double = sum
  def getVariance: Double = if (count > 1) {
    (sumSq - sum * sum / count) / (count - 1)
  } else 0
}

object Stats {
  def mapReduce(data: Traversable[Double]) = {
    Histogram.mapReduce(data) map {histogram =>
      var sum = 0.0
      var count = 0L
      var max = Double.NegativeInfinity
      var min = Double.PositiveInfinity
      var sumSq = 0.0
      for (d <- data) {
        sum += d
        count += 1
        max = d max max
        min = d min min
        sumSq += d * d
      }
      Stats(sum, count, max, min, sumSq, histogram)
    }
  }
  def reReduce(data: Traversable[Stats]) = {
    Histogram.reReduce(data map (_.histogram)) map {histogram =>
      var sum = 0.0
      var count = 0L
      var max = Double.NegativeInfinity
      var min = Double.PositiveInfinity
      var sumSq = 0.0
      for (Stats(aSum, aCount, aMax, aMin, aSumSq, _) <- data) {
        sum += aSum
        count += aCount
        max = max max aMax
        min = min min aMin
        sumSq += aSumSq
      }
      Stats(sum, count, max, min, sumSq, histogram)
    }
  }
  implicit val algo = MapReduceAlgorithm(mapReduce, reReduce)
}