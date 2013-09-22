package uk.me.jamespic.dougng.util

import org.apache.commons.math3.stat.descriptive.StatisticalSummary

case class DetailedStats(stats: Stats, histogram: Histogram) extends StatisticalSummary {
  def getMax(): Double = stats.max
  def getMean(): Double = stats.avg
  def getMin(): Double = stats.min
  def getN(): Long = stats.count
  def getStandardDeviation(): Double = stats.stdDev
  def getSum(): Double = stats.sum
  def getVariance(): Double = stats.variance
}

object DetailedStats {
  implicit val serializer = Serializer.caseClassSerializer(apply _, unapply _)
  def mapReduce(data: Traversable[Double]) = {
    for (histogram <- Histogram.mapReduce(data);
         stats <- Stats.mapReduce(data)) yield {
      DetailedStats(stats, histogram)
    }
  }
  def reReduce(data: Traversable[DetailedStats]) = {
    for (histogram <- Histogram.reReduce(data map (_.histogram));
         stats <- Stats.reReduce(data map (_.stats))) yield {
      DetailedStats(stats, histogram)
    }
  }
  implicit val algo = MapReduceAlgorithm(mapReduce, reReduce)
}