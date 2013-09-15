package uk.me.jamespic.dougng.util

import java.nio.ByteBuffer
import shapeless._
import java.util.Date
import scala.collection.generic.CanBuildFrom
import scala.collection.Traversable

object Serializer extends LowPriorityImplicits {

  def sizeof[A](implicit ser: Serializer[A]) = ser.size

  implicit object IntSerializer extends Serializer[Int] {
    val size = 4
    def serialize(value: Int, buffer: ByteBuffer) = buffer.putInt(value)
    def deserialize(buffer: ByteBuffer) = buffer.getInt()
  }

  implicit object LongSerializer extends Serializer[Long] {
    val size = 8
    def serialize(value: Long, buffer: ByteBuffer) = buffer.putLong(value)
    def deserialize(buffer: ByteBuffer) = buffer.getLong()
  }

  implicit object FloatSerializer extends Serializer[Float] {
    val size = 4
    def serialize(value: Float, buffer: ByteBuffer) = buffer.putFloat(value)
    def deserialize(buffer: ByteBuffer) = buffer.getFloat()
  }

  implicit object DoubleSerializer extends Serializer[Double] {
    val size = 8
    def serialize(value: Double, buffer: ByteBuffer) = buffer.putDouble(value)
    def deserialize(buffer: ByteBuffer) = buffer.getDouble()
  }

  implicit object ShortSerializer extends Serializer[Short] {
    val size = 2
    def serialize(value: Short, buffer: ByteBuffer) = buffer.putShort(value)
    def deserialize(buffer: ByteBuffer) = buffer.getShort()
  }

  implicit object CharSerializer extends Serializer[Char] {
    val size = 2
    def serialize(value: Char, buffer: ByteBuffer) = buffer.putChar(value)
    def deserialize(buffer: ByteBuffer) = buffer.getChar()
  }

  implicit object ByteSerializer extends Serializer[Byte] {
    val size = 1
    def serialize(value: Byte, buffer: ByteBuffer) = buffer.put(value)
    def deserialize(buffer: ByteBuffer) = buffer.get()
  }

  implicit object BooleanSerializer extends Serializer[Boolean] {
    val size = 1
    def serialize(value: Boolean, buffer: ByteBuffer) = buffer put (if (value) 1 else 0).toByte
    def deserialize(buffer: ByteBuffer) = buffer.get != 0
  }

  object NoneSerializer extends Serializer[None.type] {
    val size = 0
    def serialize(value: None.type, buffer: ByteBuffer) = ()
    def deserialize(buffer: ByteBuffer) = None
  }

  class OptionSerializer[A: Serializer] extends MultiSerializer[Option[A]](
      (0, None.getClass, NoneSerializer),
      (1, classOf[Some[A]], caseClassSerializer(Some.apply[A] _, Some.unapply[A] _))
  )

  implicit def optionSerializer[A: Serializer] = new OptionSerializer[A]

  class EitherSerializer[A, B](implicit sera: Serializer[A], serb: Serializer[B])
      extends MultiSerializer[Either[A, B]] (
          (0, classOf[Left[A, B]], caseClassSerializer(Left.apply[A, B] _, Left.unapply[A, B] _)),
          (1, classOf[Right[A, B]], caseClassSerializer(Right.apply[A, B] _, Right.unapply[A, B] _))
      )

  implicit def eitherSerializer[A: Serializer, B: Serializer] = new EitherSerializer[A, B]

  class MultiSerializer[Parent](mapping: (Byte, Class[_ <: Parent], Serializer[_ <: Parent])*)
      extends Serializer[Parent] {
    private val maxSize = mapping.map(_._3.size).max
    val size = 1 + maxSize
    private def skipSize(ser: Serializer[_]) = 0 max (maxSize - ser.size)

    def serialize(value: Parent, buf: ByteBuffer) = {
      mapping find {
        case (indicator, cls, ser) => cls.isInstance(value)
      } map {
        case (indicator, cls, ser) =>
          buf.put(indicator)
          ser.asInstanceOf[Serializer[Any]].serialize(value, buf)
          buf.position(buf.position + skipSize(ser))
      } orElse {
        throw new Exception("Unrecognised object type")
      }
    }

    def deserialize(buf: ByteBuffer) = {
      val firstByte = buf.get
      mapping find {
        case (indicator, cls, ser) => indicator == firstByte
      } map {
        case (indicator, cls, ser) =>
          val ret = ser.deserialize(buf)
          buf.position(buf.position + skipSize(ser))
          ret
      } getOrElse {
        buf.position(buf.position + maxSize)
        throw new Exception("Unrecognised object type")
      }
    }
  }

  implicit object HNilSerializer extends Serializer[HNil] {
    val size = 0
    def serialize(value: HNil, buffer: ByteBuffer) = {}
    def deserialize(buffer: ByteBuffer) = HNil
  }

  def caseClassSerializer[CC, T](apply : T => CC, unapply : CC => Option[T])(implicit ser: Serializer[T]) = {
    new Serializer[CC] {
      val size = ser.size
      def serialize(value: CC, buffer: ByteBuffer) = ser.serialize(unapply(value).get, buffer)
      def deserialize(buffer: ByteBuffer) = apply(ser.deserialize(buffer))
    }
  }

  implicit val dateSerializer = {
    implicit val iso = new Iso[Date, Long] {
      def from(l: Long) = new Date(l)
      def to(d: Date) = d.getTime()
    }
    isoSerializer[Date, Long]
  }

  implicit def hlistSerializer[A, B <: HList](implicit sera: Serializer[A], serb: Serializer[B]) = {
    new Serializer[A :: B] {
      val size = sera.size + serb.size
      def serialize(value: A :: B, buffer: ByteBuffer) = {
        sera.serialize(value.head, buffer)
        serb.serialize(value.tail, buffer)
      }
      def deserialize(buffer: ByteBuffer) = {
        val head = sera.deserialize(buffer)
        val tail = serb.deserialize(buffer)
        head :: tail
      }
    }
  }

  class FixedCollectionSerializer[A, Col <% Traversable[A]]
      (n: Int)(implicit ser: Serializer[A], cbf: CanBuildFrom[Nothing, A, Col])
      extends Serializer[Col]{
    val size = n * ser.size
    def serialize(value: Col, buffer: ByteBuffer) = {
      require(value.size == n)
      for (a <- value) {
        ser.serialize(a, buffer)
      }
    }
    def deserialize(buffer: ByteBuffer) = {
      val builder = cbf()
      builder.sizeHint(n)
      for (i <- 1 to n) {
        builder += ser.deserialize(buffer)
      }
      builder.result()
    }
  }

  class VariableCollectionSerializer[A, Col <: Traversable[A]]
      (n: Int)(implicit ser: Serializer[A], cbf: CanBuildFrom[Nothing, A, Col])
      extends Serializer[Col]{
    val size = n * (1 + ser.size)
    def serialize(value: Col, buffer: ByteBuffer) = {
      var remaining = n
      for (a <- value.take(n)) {
        buffer.put((1).toByte)
        ser.serialize(a, buffer)
        remaining -= 1
      }
      for (i <- 0 until remaining) {
        buffer.put((0).toByte)
        buffer.position(buffer.position + ser.size)
      }
    }
    def deserialize(buffer: ByteBuffer) = {
      var remaining = n
      val builder = cbf()
      while (remaining > 0) {
        val indicator = buffer.get()
        indicator match {
          case 1 =>
            builder += ser.deserialize(buffer)
            remaining -= 1
          case 0 =>
            buffer.position(buffer.position + remaining * (1 + ser.size) - 1)
            remaining = 0
        }
      }
      builder.result()
    }
  }

  def byteArraySerializer(length: Int) = new ByteArraySerializer(length)

  class ByteArraySerializer(length: Int) extends Serializer[Array[Byte]] {
    val size = length
    def serialize(value: Array[Byte], buffer: ByteBuffer) = {
      require(value.length == length)
      buffer.put(value)
    }
    def deserialize(buffer: ByteBuffer) = {
      val arr = new Array[Byte](length)
      buffer.get(arr)
      arr
    }
  }

  object ExtraImplicits {
    //Relegated to ExtraImplicits, as they tend to make hlistSerializer implicits diverge
    implicit def tupleSerializer[T <: Product, L <: HList]
        (implicit hl: HListerAux[T, L], uhl: TuplerAux[L, T], ser: Serializer[L]) = {
      implicit val iso = Iso.tupleHListIso[T, L]
      isoSerializer[T,  L]
    }
  }

  implicit def t2serializer[T1: Serializer,
    T2: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2)]]
  }

  implicit def t3serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3)]]
  }

  implicit def t4serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4)]]
  }

  implicit def t5serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5)]]
  }

  implicit def t6serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6)]]
  }

  implicit def t7serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7)]]
  }

  implicit def t8serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8)]]
  }

  implicit def t9serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9)]]
  }

  implicit def t10serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)]]
  }

  implicit def t11serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)]]
  }

  implicit def t12serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer,
    T12: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)]]
  }

  implicit def t13serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer,
    T12: Serializer,
    T13: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)]]
  }

  implicit def t14serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer,
    T12: Serializer,
    T13: Serializer,
    T14: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)]]
  }

  implicit def t15serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer,
    T12: Serializer,
    T13: Serializer,
    T14: Serializer,
    T15: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)]]
  }

  implicit def t16serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer,
    T12: Serializer,
    T13: Serializer,
    T14: Serializer,
    T15: Serializer,
    T16: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)]]
  }

  implicit def t17serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer,
    T12: Serializer,
    T13: Serializer,
    T14: Serializer,
    T15: Serializer,
    T16: Serializer,
    T17: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17)]]
  }

  implicit def t18serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer,
    T12: Serializer,
    T13: Serializer,
    T14: Serializer,
    T15: Serializer,
    T16: Serializer,
    T17: Serializer,
    T18: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18)]]
  }

  implicit def t19serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer,
    T12: Serializer,
    T13: Serializer,
    T14: Serializer,
    T15: Serializer,
    T16: Serializer,
    T17: Serializer,
    T18: Serializer,
    T19: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19)]]
  }

  implicit def t20serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer,
    T12: Serializer,
    T13: Serializer,
    T14: Serializer,
    T15: Serializer,
    T16: Serializer,
    T17: Serializer,
    T18: Serializer,
    T19: Serializer,
    T20: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20)]]
  }

  implicit def t21serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer,
    T12: Serializer,
    T13: Serializer,
    T14: Serializer,
    T15: Serializer,
    T16: Serializer,
    T17: Serializer,
    T18: Serializer,
    T19: Serializer,
    T20: Serializer,
    T21: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21)]]
  }

  implicit def t22serializer[T1: Serializer,
    T2: Serializer,
    T3: Serializer,
    T4: Serializer,
    T5: Serializer,
    T6: Serializer,
    T7: Serializer,
    T8: Serializer,
    T9: Serializer,
    T10: Serializer,
    T11: Serializer,
    T12: Serializer,
    T13: Serializer,
    T14: Serializer,
    T15: Serializer,
    T16: Serializer,
    T17: Serializer,
    T18: Serializer,
    T19: Serializer,
    T20: Serializer,
    T21: Serializer,
    T22: Serializer] = {
    import ExtraImplicits._
    implicitly[Serializer[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22)]]
  }

}

trait LowPriorityImplicits {
  def caseClassSerializer[CC, C, T <: Product, L <: HList](apply : C, unapply : CC => Option[T])
      (implicit fhl : FnHListerAux[C, L => CC], hl : HListerAux[T, L], ser: Serializer[L]) = {
    implicit val iso = Iso.hlist(apply, unapply)
    isoSerializer[CC, L]
  }

  def isoSerializer[A, B](implicit ser: Serializer[B], iso: Iso[A, B]): Serializer[A] = {
    new Serializer[A] {
      val size = ser.size
      def serialize(value: A, buffer: ByteBuffer) = ser.serialize(iso.to(value), buffer)
      def deserialize(buffer: ByteBuffer) = iso.from(ser.deserialize(buffer))
    }
  }

  def isoSerializer[A, B](iso: Iso[A, B])(implicit ser: Serializer[B]): Serializer[A] = isoSerializer(ser, iso)
}

trait Serializer[A] {
  val size: Int
  def serialize(value: A, buffer: ByteBuffer): Unit
  def deserialize(buffer: ByteBuffer): A
}
