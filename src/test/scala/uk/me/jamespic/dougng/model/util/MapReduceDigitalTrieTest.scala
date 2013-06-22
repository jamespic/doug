package uk.me.jamespic.dougng.model.util

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen
import uk.me.jamespic.dougng.util._
import MapReduce.sum


class MapReduceDigitalTrieTest extends FunSpec with ShouldMatchers with GivenWhenThen {
  describe("A MapReduceDigitalTrie") {
    describe("should be constructible") {
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
	    it("should be constructible with 100000 random items") {
	      val random = new java.util.Random(0)
	      testConstructPaired(Traversable.fill(100000)(random.nextLong))
	    }
    }

    describe("containing 1 -> 1, 1 -> 2, 255 -> 7, 256 -> 8, 65535 -> 9, 65536 -> 10, 65536 -> 11") {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      instance ++= Seq(1L -> 1L,
                       1L -> 2L,
                       255L -> 7L,
                       256L -> 8L,
                       65535L -> 9L,
                       65536L -> 10L,
                       65536L -> 11L)
      it("should find two entries at 1L") {
    	instance.get(1L).toSet should equal(Set(1L, 2L))
      }
      it("should find one entry at 255L") {
        instance.get(255L).toSet should equal(Set(7L))
      }
      it("should find all entries between Long.MaxValue and Long.MinValue") {
        val values = instance.getBetween(Long.MinValue, Long.MaxValue).toSet
        values should equal(Set(1L, 2L, 7L, 8L, 9L, 10L, 11L))
      }
      it("should find three entries between 1 and 255") {
        instance.getBetween(1L, 255L).toSet should equal(Set(1L, 2L, 7L))
      }
      it("should find two entries between 255 and 256") {
        instance.getBetween(255L, 256L).toSet should equal(Set(7L, 8L))
      }
      it("should find three entries between 65535 and 65536") {
        instance.getBetween(65535L, 65536L).toSet should equal(Set(9L, 10L, 11L))
      }
    }

    it("should not recalculate the same summaries") {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      for (i <- 0L to 5898240L by 65536L) {
        instance += i -> i
      }
      val summary = instance.summary
      info(s"Calculated summary: $summary")
      instance.prettyPrint.contains("Summary: None") should be(false)
      val sumBetween = instance.summaryBetween(100L, 999999999L)
      info(s"Calculated summary between: $sumBetween")
    }

    describe("Containing the numbers from 1 to 1000") {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      instance ++= (for (i <- 1L to 1000L) yield i -> i)
      instance.summary

      it("should have a minimum key of 1") {
        instance.minKey should equal(Some(1L))
      }

      it("should have a maximum key of 1000") {
        instance.maxKey should equal(Some(1000L))
      }

      it("should have a summary of 55 between 1 and 10") {
        instance.summaryBetween(1L, 10L) should equal(Some(55L))
      }

      it("should have a summary of 5050 between 1 and 100") {
        instance.summaryBetween(1L, 100L) should equal(Some(5050L))
      }

      it("should have a summary of 500500 between 1 and 1000") {
        instance.summaryBetween(1L, 1000L) should equal(Some(500500L))
      }

      it("should have a summary of 1 between Long.MinValue and 1") {
        instance.summaryBetween(Long.MinValue, 1L) should equal(Some(1L))
      }

      it("should have no summary between Long.MinValue and 0") {
        instance.summaryBetween(Long.MinValue, 0L) should equal(None)
      }

      it("should have no summary between 1001 and Long.MaxValue") {
        instance.summaryBetween(1001L, Long.MaxValue) should equal(None)
      }

      it("should have a summary of 500500 between Long.MinValue and Long.MaxValue") {
        instance.summaryBetween(Long.MinValue, Long.MaxValue) should equal(Some(500500L))
      }

      it("should have a summary of 181497 between 4 and 602") {
        instance.summaryBetween(4L, 602L) should equal(Some(181497L))
      }

      it("should have a summary of 98176 between 256 and 511") {
        instance.summaryBetween(256L, 511L) should equal(Some(98176L))
      }

      it("should have a summary of 98943 between 256 and 511") {
        instance.summaryBetween(255L, 512L) should equal(Some(98943L))
      }
    }
  }

  def testConstruct(source: Traversable[(Long, Long)]) = {
    val alloc = new MappedAllocator
    //val alloc = new ChannelAllocator
    val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
    time("Constructed") {
      for (i <- source) {
        instance += i
      }
    }
    require(instance.toSet == source.toSet)
    if (instance.size > 1) {
      instance.toSeq.sliding(2).forall {case Seq((k1, v1), (k2, v2)) => k1 <= k2} should be(true)
    }

    time("Summarized") {
      instance.summary should equal(Some(source.map(_._2).sum))
    }

    time("Resummarized")(instance.summary)

    instance.close
  }
  def testConstructPaired(source: Traversable[Long]) = {
    testConstruct(source map (i => i -> i))
  }
  def time[A](desc: String)(f: => A) = {
    val start = System.currentTimeMillis
    val ret = f
    val end = System.currentTimeMillis
    info(s"$desc in ${(end - start) / 1000.0f} seconds")
    ret
  }
}