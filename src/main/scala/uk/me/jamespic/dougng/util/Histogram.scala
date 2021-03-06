package uk.me.jamespic.dougng.util

import scala.collection.mutable.ArrayBuffer
import java.nio.ByteBuffer
import scala.collection.immutable.SortedSet
import scala.collection.immutable.SortedMap

class Histogram private(private val start: DoubleRep, private val data: Array[Int]) {
  import Histogram._
  private def sig = start.significand
  private def exp = start.exponent
  def end = start + bars
  def highest = start + highestNonZeroIndex
  def entries = {
    var lastCount = 0
    for (i <- 0 until 64) yield {
      val total = data(i)
      val barHeight = total - lastCount
      lastCount = total
      (Range(sig + i, sig + i + 1, exp), barHeight)
    }
  }

  private def countLessThan(limit: DoubleRep) = {
    require(limit.exponent >= start.exponent)
    if (limit + 1 < start) 0
    else if (limit >= end) data.last
    else {
      val normalisedLimit = ((limit + 1) withExponent start.exponent) + (-1)
      val index = normalisedLimit.significand - start.significand
      if (index < 0) 0
      else if (index >= bars) data.last
      else data(index.toInt)
    }
  }

  def nonZero = entries filter (_._2 != 0)
  def size = data.last

  override def toString = {
    val e = entries.slice(0, highestNonZeroIndex + 1)
    val rangeColLength = e.map(_._1.toString.length).max
    val longestBar = e.map(_._2).max
    val rows = for ((r, n) <- e) yield {
      val range = r.toString.padTo(rangeColLength, ' ')
      val bars = "#" * (n * stringBarWidth / longestBar)
      s"$range: $bars"
    }
    rows.mkString("\n")
  }

  def percentile = new Percentile

  private def highestNonZeroIndex = {
    var i = 0
    for (j <- 1 until bars) {
      if (data(j) > data(j - 1)) i = j
    }
    i
  }

  private def range = {
    // We abuse range, to be a range of chunks. This is for internal use, and we
    // don't want to expose a confusing API, so we make it package private
    Range(sig, sig + highestNonZeroIndex, exp)
  }

  class Percentile private[Histogram] {
    private case class Segment(startPerc: Double, endPerc: Double, startVal: Double, endVal: Double)
    private val percentiles = {
      val max = Histogram.this.size.toDouble
      var counter = 0
      val r = for ((r, c) <- nonZero) yield {
        val startVal = r.from.toDouble
        val endVal = r.to.toDouble
        val startPerc = counter / max
        counter += c
        val endPerc = counter / max
        startPerc -> Segment(startPerc, endPerc, startVal, endVal)
      }
      SortedMap(r:_*)
    }
    def apply(p: Double) = {
      require(0.0 <= p && p <= 1.0)
      val Segment(startPerc, endPerc, startVal, endVal) = percentiles.to(p).last._2
      val segPos = (p - startPerc) / (endPerc - startPerc)
      segPos * endVal + (1 - segPos) * startVal
    }
  }
}

object Histogram {
  val barsExponent = 6
  val bars = pow2(barsExponent).toInt
  val stringBarWidth = 40
  def selectExponent(min: Double, max: Double): Int = {
    val minRep = DoubleRep(min)
    val maxRep = DoubleRep(max)
    val barRep = DoubleRep((max - min) / (bars - 1))
    val idealExponent = barRep.logRoundUp
    idealExponent max minRep.minExponent max maxRep.minExponent
  }

  def selectRange(data: Traversable[Histogram]) = {
    val range = data.view map (_.range) reduce (_ ++ _)
    val Range(min, max, exp) = range
    val barRep = DoubleRep((max - min), exp - barsExponent)
    val idealExponent = barRep.logRoundUp
    if (idealExponent > exp) {
      val shift = idealExponent - exp
      Range(min >>| shift, max >>| shift, idealExponent)
    } else range
  }

  case class Range(min: Long, max: Long, exponent: Int) {
    def ++(that: Range) = {
      val (e, min1, min2, max1, max2) = if (this.exponent >= that.exponent) {
        val shift = this.exponent - that.exponent
        (this.exponent, this.min, that.min >>| shift, this.max, that.max >>| shift)
      } else {
        val shift = that.exponent - this.exponent
        (that.exponent, this.min >>| shift, that.min, this.max >>| shift, that.max)
      }
      Range(min1 min min2, max1 max max2, e)
    }

    def from = DoubleRep(min, exponent)
    def to = DoubleRep(max, exponent)
    override def toString = s"Range(${from.toDouble}-${to.toDouble})"
  }

  def mapReduce(data: Traversable[Double]) = {
    if (data.isEmpty) None else Some(apply(data))
  }
  def reReduce(data: Traversable[Histogram]) = {
    if (data.isEmpty) None else Some(merge(data))
  }

  def apply(data: Traversable[Double]) = {
    var min = Double.MaxValue
    var max = Double.MinValue
    for (d <- data) {
      if (d < min) min = d
      if (d > max) max = d
    }
    val exponent = selectExponent(min, max)
    val minRep = DoubleRep(min) withExponent(exponent)
    val minSig = minRep.significand
    val buffer = new Array[Int](bars)
    for (d <- data) {
      val DoubleRep(valSig, _) = DoubleRep(d) withExponent(exponent)
      buffer((valSig - minSig).toInt) += 1
    }
    // Buffer is an accumulator, so we go through it and accumulate
    var i = 1
    while (i < bars) {
      buffer(i) += buffer(i - 1)
      i += 1
    }
    new Histogram(minRep, buffer)
  }

  def merge(data: Traversable[Histogram]) = {
    val Range(minSig, maxSig, exponent) = selectRange(data)
    val start = DoubleRep(minSig, exponent)
    val buffer = new Array[Int](bars)
    for (hist <- data; i <- 0 until bars) {
      buffer(i) += hist.countLessThan(start + i)
    }
    new Histogram(start, buffer)
  }

  implicit object HistogramSerializer extends Serializer[Histogram] {
    import Serializer.sizeof
    private val doubleRepSerializer: Serializer[DoubleRep] = implicitly
    private val arraySerializer = new Serializer.FixedCollectionSerializer[Int, Array[Int]](bars)
    val size = sizeof[DoubleRep] + bars * sizeof[Int]
    def serialize(value: Histogram, buffer: ByteBuffer): Unit = {
      doubleRepSerializer.serialize(value.start, buffer)
      arraySerializer.serialize(value.data, buffer)
    }
    def deserialize(buffer: ByteBuffer): Histogram = {
      val start = doubleRepSerializer.deserialize(buffer)
      val data = arraySerializer.deserialize(buffer)
      new Histogram(start, data)
    }
  }

  implicit object HistogramMapReduce extends MapReduceAlgorithm[Double, Histogram](mapReduce, reReduce)
}