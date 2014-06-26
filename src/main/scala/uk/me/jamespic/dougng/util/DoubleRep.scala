package uk.me.jamespic.dougng.util
import java.lang.{Double => JDouble}
import scala.annotation.tailrec

case class DoubleRep(significand: Long, exponent: Int) extends Ordered[DoubleRep] {
  import DoubleRep._
  def sign = significand >>> signShift
  def signum = 1 - 2 * sign
  def unsignedSignificand = signum * significand

  def toDouble = {
    if (significand == 0L) 0.0
    else {
      val (normUnSignificand, normExponent) = normalise(unsignedSignificand, exponent)
      val biasedExponent = if (normExponent == minExponent && ((normUnSignificand & significandTopBit) == 0L)) {
        0
      } else normExponent + exponentShift + bias
      val bits = (sign << signShift) | (biasedExponent.toLong << exponentShift) | (normUnSignificand & significandMask)
      JDouble.longBitsToDouble(bits)
    }
  }

  def normal = {
    val (normUnSignificand, normExponent) = normalise(unsignedSignificand, exponent)
    DoubleRep(signum * normUnSignificand, normExponent)
  }

  def +(d: Long) = d match {
    case 0L => this
    case d => DoubleRep(significand + d, exponent)
  }

  def <>(shift: Int) = {
    shift match {
      case 0 => this
      case shift if shift > 0 => DoubleRep(significand >>| shift, exponent + shift)
      case shift if shift < 0 => DoubleRep(significand << -shift, exponent + shift)
    }
  }

  def withExponent(exp: Int) = {
    this <> (exp - this.exponent)
  }

  def minExponent = exponent + log2(unsignedSignificand) - 62

  def withMinExponent = {
    this withExponent minExponent
  }

  def log = {
    if (significand == 0) Int.MinValue
    else log2(unsignedSignificand) + exponent
  }
  def logRoundUp = log + 1

  def compare(that: DoubleRep) = {
    //compareBySigns(that) orElse (compareByLogs(that) orElse compareBySignificands(that))
    compareBySigns(that) match {
      case 0 => compareByLogs(that) match {
        case 0 => compareBySignificands(that)
        case r2 => r2
      }
      case r1 => r1
    }
  }

  private def compareBySigns(that: DoubleRep) = {
    this.signum compare that.signum
  }

  private def compareByLogs(that: DoubleRep) = {
    // Assume both have same sign. We don't validate this, as it's a private method
    //require(this.sign == that.sign)
    // Assume both positive, for now. If x has a bigger log than y, it's bigger
    val logCmp = this.log compare that.log
    // If they're negative, we reverse the comparison
    logCmp * this.signum.toInt
  }

  private def compareBySignificands(that: DoubleRep) = {
    val bestExponent = this.exponent min that.exponent
    val xSig = (this withExponent bestExponent).significand
    val ySig = (that withExponent bestExponent).significand
    xSig compare ySig
  }

  override def toString = s"$significand x 2^$exponent (${toDouble})"
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