package uk.me.jamespic.dougng.util

trait Allocator extends Hibernatable {
  type HandleType
  implicit val handleSerializer: Serializer[HandleType]
  val nullHandle: HandleType
  def apply(size: Int): Storage
  def apply[A](implicit ser: Serializer[A]): Storage = apply(ser.size)
  def storage(handle: HandleType, size: Int): Storage
  def storage[A](handle:  HandleType)(implicit ser: Serializer[A]): Storage = storage(handle, ser.size)
  def close

  trait Storage {
    def handle: HandleType
    def write[A](off: Int, value: A)(implicit ser: Serializer[A]): Unit
    def read[A](off: Int)(implicit ser:Serializer[A]): A
    def free: Unit
  }
}