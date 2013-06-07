package uk.me.jamespic.dougng.model.util

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen
import uk.me.jamespic.dougng.util._
import MapReduce.sum


class MapReduceDigitalTrieTest extends FunSpec with ShouldMatchers with GivenWhenThen {
  describe("A MapReduceDigitalTrie") {
    it("should be contructible with 1 item") {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      instance += 1L -> 1L
      println(instance.prettyPrint)
    }
    it("should be contructible with 2 sequential items") {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      instance += 1L -> 1L
      instance += 2L -> 2L
      println(instance.prettyPrint)
    }
    it("should be contructible with 2 identical items") {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      instance += 1L -> 1L
      instance += 1L -> 1L
      println(instance.prettyPrint)
    }
    it("should be contructible with 20 sequential items") {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      for (i <- 1L to 20L) {
        instance += i -> i
      }
      println(instance.prettyPrint)
    }
    it("should be contructible with 90 sequential items") {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      for (i <- 1L to 90L) {
        instance += i -> i
      }
      println(instance.prettyPrint)
    }
    
    it("should be constructible with 2 items in different buckets") {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      instance += 1L -> 1L
      println(instance.prettyPrint)
      instance += 257L -> 257L
      println(instance.prettyPrint)
    }
    
    it("should be constructible with 32 items in different buckets") {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      for (i <- 0L to 4096L by 128) {
        //println(s"Adding $i")
        instance += i -> i
        //println(instance.prettyPrint)
      }
      println(instance.prettyPrint)
    }
    
    it("should be constructible with 180 items in different buckets") {
      val alloc = new ChannelAllocator
      val instance = new MapReduceDigitalTrie[Long, Long](sum _, sum _)(alloc)
      for (i <- 0L to 23040L by 128) {
        instance += i -> i
      }
      println(instance.prettyPrint)
    }

  }
}