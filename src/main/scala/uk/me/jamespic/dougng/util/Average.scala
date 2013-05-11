package uk.me.jamespic.dougng.util

case class Average(sum: Double, count: Long) {
  def +(o: Double) = Average(sum + o, count + 1)
  def avg = if (count > 0) {sum / count} else {0}
}

object Average extends Average(0, 0)