package uk.me.jamespic.dougng.util

object Allocator {
  private val MappedPlatforms = Set(("Linux","64"), ("Windows","64"))
  def apply() = {
    import scala.collection.JavaConversions._
    (for (platform <- Option(System.getProperty("os.name"));
         bits <- Option(System.getProperty("sun.arch.data.model"));
         if MappedPlatforms contains (platform, bits)) yield {
      new MappedAllocator
    }) getOrElse {
      //new ChannelAllocator
      new PageCacheAllocator
    }
  }
}

trait Allocator {
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