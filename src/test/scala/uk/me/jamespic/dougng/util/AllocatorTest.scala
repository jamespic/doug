package uk.me.jamespic.dougng.util

import org.scalatest.FunSpecLike
import org.scalatest.Matchers
import org.scalatest.GivenWhenThen

class AllocatorTest extends FunSpecLike with Matchers with GivenWhenThen {
  describe("A Mapped Allocator") {
    it("should pass the standard test") {
      standardTest(new MappedAllocator(64))
    }
  }
  describe("A Channel Allocator") {
    it("should pass the standard test") {
      standardTest(new ChannelAllocator)
    }
  }
  describe("A PageCache Allocator") {
    it("should pass the standard test") {
      standardTest(new PageCacheAllocator(16))
    }
  }

  def standardTest(allocator: Allocator) = {
    val intStorage = allocator[Int]
    intStorage.read[Int](0) should equal(0)
    intStorage.write(0, 100)
    intStorage.read[Int](0) should equal(100)
    val longStorage = allocator[Long]
    longStorage.read[Long](0) should equal(0L)
    longStorage.write(0, 200L)
    longStorage.read[Long](0) should equal(200L)
    intStorage.free
    val intStorage2 = allocator[Int]
    intStorage2.read[Int](0) should equal(0)
    intStorage2.write(0, 100)
    intStorage2.read[Int](0) should equal(100)
    val longHandle = longStorage.handle
    val longStorage2 = allocator.storage[Long](longHandle)
    longStorage2.read[Long](0) should equal(200L)
    
    intStorage2.free
    val tupleStorage = allocator[(Int, Long)]
    tupleStorage.read[Long](4) should equal(0L)
    tupleStorage.write(4, 4L)
    tupleStorage.read[Long](4) should equal(4L)
    
    
    // Allocate a large number (enough to cross into a new block, or bust the cache) and test values are updated
    val storages = for (i <- 0 until 63) yield {
      (i, allocator[Int])
    }
    
    for ((i, storage) <- storages) {
      storage.read[Int](0) should equal(0)
      storage.write(0, i)
    }
    
    val handles = for ((i, storage) <- storages) yield {
      storage.read[Int](0) should equal(i)
      (i, storage.handle)
    }
    
    for ((i, handle) <- handles) {
      val storage = allocator.storage[Int](handle)
      storage.read[Int](0) should equal(i)
    }
    
    allocator.close
  }
}