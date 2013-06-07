package uk.me.jamespic.dougng.model.util

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen
import uk.me.jamespic.dougng.util._
import MapReduce.sum


class MapReduceDigitalTrieTest extends FunSpec with ShouldMatchers with GivenWhenThen {
  describe("A MapReduceDigitalTrie") {
    def testConstruct(source: Traversable[(Long, Long)]) = {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      val start = System.currentTimeMillis
      for (i <- source) {
        instance += i
      }
      val end = System.currentTimeMillis
      info(s"Constructed in ${(end - start) / 1000.0f} seconds")
      //println(instance.prettyPrint)
      instance.toSet should equal(source.toSet)
      if (instance.size > 1) {
        instance.toSeq.sliding(2).forall {case Seq((k1, v1), (k2, v2)) => k1 <= k2} should be(true)
      }
      instance.close
    }
    def testConstructPaired(source: Traversable[Long]) = {
      testConstruct(source map (i => i -> i))
    }
    it("should be contructible with 1 item") {
      testConstruct(Seq(1L -> 1L))
    }
    it("should be contructible with 2 sequential items") {
      testConstruct(Seq(1L -> 1L, 2L -> 2L))
    }
    it("should be contructible with 2 similar items") {
      testConstruct(Seq(1L -> 1L, 1L -> 2L))
    }
    it("should be contructible with 20 sequential items") {
      testConstruct(Seq(1L -> 1L, 2L -> 2L))
    }
    it("should be contructible with 90 sequential items") {
      testConstructPaired(1L to 90L)
    }

    it("should be constructible with 2 items in different buckets") {
      testConstruct(Seq(1L -> 1L, 257L -> 257L))
    }

    it("should be constructible with 32 items in different buckets") {
      testConstructPaired(0L to 4096L by 128)
    }

    it("should be constructible with 180 items in different buckets") {
      testConstructPaired(0L to 23040L by 128)
    }

    it("should be constructible with 20000 random items") {
      val random = new java.util.Random(0)
      testConstructPaired(Traversable.fill(20000)(random.nextLong))
    }

  }
}