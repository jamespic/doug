package uk.me.jamespic.dougng.model.util

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen
import uk.me.jamespic.dougng.util._

class MapReduceBRTreeTest extends FunSpec with ShouldMatchers with GivenWhenThen {
  describe("A MapReduceBRTree") {
    import MapReduce.sum
    it("should be constructible") {
      val alloc = new DebuggingAllocator()
      val instance = new MapReduceBRTree[Long, Long, Long](sum _, sum _)(alloc)

      instance += 1L -> 1L
      instance should have size(1)
      instance.toList should equal(List(1L -> 1L))

      instance += 2L -> 2L
      instance should have size(2)
      instance.toList should equal(List(1L -> 1L, 2L -> 2L))

      for (i <- 3L to 25L) {
        instance += i -> i
      }

      println(instance)
      instance should have size(25)
      instance.toList should equal(for (i <- 1L to 25L) yield i -> i)

      for (i <- 26L to 1000L) {
        instance += i -> i
      }

      instance should have size(1000)
      instance.toList should equal(for (i <- 1L to 1000L) yield i -> i)
    }

    it("should be fast") {
      val alloc = new MappedAllocator()
      val instance = new MapReduceBRTree[Long, Long, Long](sum _, sum _)(alloc)

      for (i <- 1L to 1000000L) {
        instance += i -> i
      }
    }
    
    it("should be fast with random data") {
      val alloc = new MappedAllocator()
      val instance = new MapReduceBRTree[Long, Long, Long](sum _, sum _)(alloc)
      val rand = new java.util.Random
      
      for (i <- 1L to 1000000L) {
        val j = rand.nextLong()
        instance += j -> j
      }
    }
  }
}