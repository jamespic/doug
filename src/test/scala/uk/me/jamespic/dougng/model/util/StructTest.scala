package uk.me.jamespic.dougng.model.util

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen
import uk.me.jamespic.dougng.util._


class StructTest extends FunSpec with ShouldMatchers with GivenWhenThen {
  describe("A Struct") {
    class SubStruct(storage: Allocator#Storage, offset: Int) extends Struct[SubStruct](storage, offset) {
      val x = __var__[Double]
      val y = __buffer__[(Short, Short)](2)
    }
    implicit val subInfo = StructInfo(new SubStruct(_,_))
    class BasicStruct(storage: Allocator#Storage, offset: Int) extends Struct[BasicStruct](storage, offset) {
      val a = __var__[Int]
      val b = __buffer__[Int](2)
      val f = __array__[SubStruct](2)
      val e = __struct__[SubStruct]
      val c = __var__[Long]
      val h = __struct_buffer__[SubStruct](2)
      val d = __buffer__[Long](3)
      val g = __buffer__[Int](5)
    }

    implicit val basicInfo = StructInfo(new BasicStruct(_,_))

    it("should allow all its variables to be updated") {
      val alloc = Allocator()
      val instance = basicInfo.allocate(alloc)
      instance.a() = 1
      instance.b += 2
      instance.b += 3
      intercept[RuntimeException] {
        instance.b += 100
      }
      instance.c() = 4L
      instance.d += 5L
      instance.d += 6L
      instance.e.x() = 7L
      instance.e.y += 8.toShort -> 9.toShort
      instance.e.y += 10.toShort -> 11.toShort
      instance.f(0).x() = 12L
      instance.f(1).x() = 13L
      val h = subInfo.inMemory
      h.x() = 14L
      instance.h += h

      instance.g += 1
      2 +=: instance.g
      instance.g.insertAll(1, Seq(3, 4, 5))
      instance.g(2) = 6
      instance.g.remove(3) should equal(5)
      instance.g should equal(Seq(2, 3, 6, 1))

      instance.a() should equal(1)
      instance.b should equal(Seq(2, 3))
      instance.c() should equal(4L)
      instance.d should equal(Seq(5L, 6L))
      instance.e.x() should equal(7L)
      instance.e.y should equal(Seq(8.toShort -> 9.toShort, 10.toShort -> 11.toShort))
      instance.f(0).x() should equal(12L)
      instance.f(1).x() should equal(13L)
      instance.f(0).y should be('empty)
      instance.h.size should equal(1)
      instance.h(0).x() should equal(14L)
    }
  }
}