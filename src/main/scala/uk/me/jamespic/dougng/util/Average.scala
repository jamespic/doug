package uk.me.jamespic.dougng.util

case class Average(sum: Double, count: Long) extends Combinable[Double, Average]{
  def +(o: Double) = Average(sum + o, count + 1)
  def ++(a: Average) = Average(sum + a.sum, count + a.count)
  def avg = if (count > 0) {sum / count} else {0}
}

object Average extends Average(0, 0) with Start[Double, Average] {
  implicit val start: Start[Double, Average] with Average = this
}