package uk.me.jamespic.dougng.util

import org.scalatest.FunSpecLike
import org.scalatest.Matchers
import org.scalatest.GivenWhenThen

class MapReduceBRTreeTest extends FunSpecLike with Matchers with GivenWhenThen {
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

    it("should be randomly constructible") {
      val alloc = new DebuggingAllocator()
      val instance = new MapReduceBRTree[Long, Long, Long](sum _, sum _)(alloc)

      val rand = new scala.util.Random
      rand.setSeed(0L) // Set seed deterministically

      val entries = for (i <- 1L to 1013L) yield i -> i
      for (e <- rand.shuffle(entries)) {
        instance += e
      }
      instance.getBetween(Some(380L), Some(671L)).toList should equal(for (i <- 380L to 671L) yield i -> i)
    }

    it("should be summarizable") {
      val alloc = new DebuggingAllocator()
      val instance = new MapReduceBRTree[Long, Long, Long](sum _, sum _)(alloc)

      for (i <- 1L to 1000L) {
        instance += i -> i
      }

      instance.summary should equal(Some(500500L))
      instance.summaryBetween(Some(10L), Some(100L)) should equal(Some(5005L))
      instance.summaryBetween(None, Some(0L)) should equal(None)
      instance.summaryBetween(None, Some(1L)) should equal(Some(1L))
      instance.summaryBetween(Some(1000L), None) should equal(Some(1000L))
    }

    it("should be fast") {
      val alloc = new PageCacheAllocator()
      val instance = new MapReduceBRTree[Long, Long, Long](sum _, sum _)(alloc)

      time {
        for (i <- 1L to 100000L) {
          instance += i -> i
        }
      }
      time (instance.summary)
    }

    it("should be fast with random data") {
      val alloc = new PageCacheAllocator()
      val instance = new MapReduceBRTree[Long, Long, Long](sum _, sum _)(alloc)
      val rand = new java.util.Random

      time {
        for (i <- 1L to 100000L) {
          val j = rand.nextLong()
          instance += j -> j
        }
      }
      time (instance.summary)
    }
  }
}