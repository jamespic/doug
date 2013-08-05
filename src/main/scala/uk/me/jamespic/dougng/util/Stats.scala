package uk.me.jamespic.dougng.util

import math.sqrt

case class Stats(sum: Double, count: Long, max: Double, min: Double, sumSq: Double) extends Combinable[Double, Stats] {
  def +(o: Double) = Stats(sum + o, count + 1, max max o, min min o, sumSq + o * o)
  def ++(o: Stats) = Stats(sum + o.sum, count + o.count, max max o.max, min min o.min, sumSq + o.sumSq)
  def avg = if (count > 0) {sum / count} else {0}
  def popStdDev = if (count > 1) {
    sqrt((sumSq - sum * sum / count) / (count - 1))
  } else 0
}

object Stats extends Stats(0,0,Double.NegativeInfinity,Double.PositiveInfinity,0) with Start[Double, Stats] {
  implicit val serializer = Serializer.caseClassSerializer(Stats.apply _, Stats.unapply _)
  def single(v: Double): Stats = Stats(v, 1, v, v, v * v)

  implicit val start: Start[Double,  Stats] with Stats = this
}