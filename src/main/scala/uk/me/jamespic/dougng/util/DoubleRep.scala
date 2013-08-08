package uk.me.jamespic.dougng.util
import java.lang.{Double => JDouble}
import scala.annotation.tailrec

case class DoubleRep(significand: Long, exponent: Int) {
  import DoubleRep._
  def sign = if (significand < 0) 1 else 0
  def signum = significand.signum
  def unsignedSignificand = significand.abs

  def toDouble = {
    val (normUnSignificand, normExponent) = normalise(unsignedSignificand, exponent)
    val biasedExponent = if (normExponent == minExponent && ((normUnSignificand & significandTopBit) == 0L)) {
      0
    } else normExponent + exponentShift + bias
    val bits = (sign << signShift) | (biasedExponent.toLong << exponentShift) | (normUnSignificand & significandMask)
    JDouble.longBitsToDouble(bits)
  }

  def normal = {
    val (normUnSignificand, normExponent) = normalise(unsignedSignificand, exponent)
    DoubleRep(signum * normUnSignificand, normExponent)
  }
}

object DoubleRep {
  val significandMask = 0xfffffffffffffL
  val exponentMask = 0x7ff
  val exponentShift = 52
  val signShift = 63
  val significandTopBit = 0x10000000000000L
  val bias = 1023
  val excessSignificandMask = 0xffe0000000000000L
  val minExponent = 1 - bias - exponentShift

  def unapply(d: Double): Option[(Long, Int)] = unapply(apply(d))

  def apply(d: Double): DoubleRep = {
    if (JDouble.isNaN(d) || JDouble.isInfinite(d)) {
      throw new IllegalArgumentException(s"$d is not finite")
    }
    val longRep = JDouble.doubleToLongBits(d)

    val rawSign = longRep >>> signShift
    val rawSignificand = longRep & significandMask
    val rawExponent = (longRep >>> exponentShift) & exponentMask
    val exponent = if (rawExponent != 0L) {
      (rawExponent - bias - exponentShift).toInt
    } else minExponent
    val significand = if (rawExponent != 0L) rawSignificand | significandTopBit else rawSignificand
    val sign = if (rawSign == 0) 1 else -1
    DoubleRep(sign * significand, exponent)
  }

  @tailrec private def normalise(unsignedSignificand: Long, exponent: Int): (Long, Int) = {
    if ((unsignedSignificand & excessSignificandMask) != 0) {
      normalise(unsignedSignificand >>> 1, exponent + 1)
    } else if ((unsignedSignificand & significandTopBit) == significandTopBit || exponent == minExponent) {
      (unsignedSignificand, exponent)
    } else {
      normalise(unsignedSignificand << 1, exponent - 1)
    }
  }
}