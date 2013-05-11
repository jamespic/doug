package uk.me.jamespic.dougng.util

import math.sqrt

case class Stats(sum: Double, count: Long, max: Double, min: Double, sumSq: Double) {
  def +(o: Double) = Stats(sum + o, count + 1, max max o, min min 0, sumSq + o * o)
  def avg = if (count > 0) {sum / count} else {0}
  def popStdDev = if (count > 1) {sqrt((sum * sum - sumSq) / (count - 1))} else 0
}

object Stats extends Stats(0,0,Double.NegativeInfinity,Double.PositiveInfinity,0)