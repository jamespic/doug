package uk.me.jamespic.dougng.util

import java.nio.ByteBuffer
import shapeless._
import java.util.Date
import scala.collection.generic.CanBuildFrom
import scala.collection.Traversable

object Serializer extends LowPrioritySerializerImplicits {

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
      (None.getClass, NoneSerializer),
      (classOf[Some[A]], implicitly[Serializer[Some[A]]] /*caseClassSerializer(Some.apply[A] _, Some.unapply[A] _)*/)
  )

  implicit def optionSerializer[A: Serializer] = new OptionSerializer[A]

  class EitherSerializer[A, B](implicit sera: Serializer[A], serb: Serializer[B])
      extends MultiSerializer[Either[A, B]] (
          (classOf[Left[A, B]], implicitly[Serializer[Left[A, B]]]),
          (classOf[Right[A, B]], implicitly[Serializer[Right[A, B]]])
      )

  implicit def eitherSerializer[A: Serializer, B: Serializer] = new EitherSerializer[A, B]

  class MultiSerializer[Parent](mapping: (Class[_ <: Parent], Serializer[_ <: Parent])*)
      extends Serializer[Parent] {
    private val maxSize = mapping.map(_._2.size).max
    val size = 1 + maxSize
    private def skipSize(ser: Serializer[_]) = 0 max (maxSize - ser.size)

    def serialize(value: Parent, buf: ByteBuffer) = {
      val i = mapping indexWhere {
        case (cls, ser) => cls.isInstance(value)
      }
      mapping.lift(i) map {
        case (cls, ser) =>
          buf.put(i.toByte)
          ser.asInstanceOf[Serializer[Any]].serialize(value, buf)
          buf.position(buf.position + skipSize(ser))
      } orElse {
        throw new Exception("Unrecognised object type")
      }
    }

    def deserialize(buf: ByteBuffer) = {
      val firstByte = buf.get
      mapping.lift(firstByte.toInt) map {
        case (cls, ser) =>
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

  def combine[X: Serializer, Y: Serializer] = implicitly[Serializer[(X, Y)]]

  implicit val dateSerializer = {
    implicit val iso = new Generic[Date] {
      type Repr = Long
      def from(l: Long) = new Date(l)
      def to(d: Date) = d.getTime()
    }
    genericSerializer[Date, Long]
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
}

trait LowPrioritySerializerImplicits {
  class GenericSerializer[T, X](implicit aux: Generic.Aux[T, X], ser: Serializer[X]) extends Serializer[T] {
    override val size: Int = ser.size
    override def serialize(value: T, buffer: ByteBuffer): Unit = ser.serialize(aux.to(value), buffer)
    override def deserialize(buffer: ByteBuffer): T = aux.from(ser.deserialize(buffer))
  }

  implicit def genericSerializer[T, X](implicit aux: Generic.Aux[T, X], ser: Serializer[X]) = new GenericSerializer[T, X]
  def generic[T](generic: Generic[T])(implicit ser: Serializer[generic.Repr]) = {
    genericSerializer[T, generic.Repr](generic, ser)
  }
}

trait Serializer[A] {
  val size: Int
  def serialize(value: A, buffer: ByteBuffer): Unit
  def deserialize(buffer: ByteBuffer): A
}
