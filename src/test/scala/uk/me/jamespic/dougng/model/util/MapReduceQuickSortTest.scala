package uk.me.jamespic.dougng.model.util

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen
import uk.me.jamespic.dougng.util.MapReduceQuickSort

class MapReduceQuickSortTest extends FunSpec with ShouldMatchers with GivenWhenThen {
  describe("A MapReduceQuickSort") {
    describe("containing the integers between 1 and 10000") {
      val values = for (i <- 1 to 10000) yield (i, i)
      val instance = new MapReduceQuickSort[Int, Int, Int](values, identity, _ + _)

      it("should have an overall summary value of 50005000") {
        instance.summaryBetween(1, 10000) should equal(Some(50005000))
      }
      it("should have a summary value of 495500 between 100 and 1000") {
        instance.summaryBetween(100, 1000) should equal(Some(495550))
      }
      it("should have a null summary value between -100 and 0") {
        instance.summaryBetween(-100, 0) should equal(None)
      }
      it("should have a null summary value between 1000000 and 2000000") {
        instance.summaryBetween(1000000, 2000000) should equal(None)
      }
    }
    describe("containing the integers from 0 to 10000 in steps of 10") {
      val values = for (i <- 0 to 10000 by 10) yield (i, i)
      val instance = new MapReduceQuickSort[Int, Int, Int](values, identity, _ + _)

      it("should calculate leastUpperBound correctly") {
        instance.leastUpperBound(55) should equal(Some(60))
        instance.leastUpperBound(100) should equal(Some(100))
        instance.leastUpperBound(0) should equal(Some(0))
        instance.leastUpperBound(-1) should equal(Some(0))
        instance.leastUpperBound(10000) should equal(Some(10000))
        instance.leastUpperBound(10001) should equal(None)
      }
      it("should calculate greatestLowerBound correctly") {
        instance.greatestLowerBound(55) should equal(Some(50))
        instance.greatestLowerBound(100) should equal(Some(100))
        instance.greatestLowerBound(0) should equal(Some(0))
        instance.greatestLowerBound(-1) should equal(None)
        instance.greatestLowerBound(10000) should equal(Some(10000))
        instance.greatestLowerBound(10001) should equal(Some(10000))
      }
    }
    it("should survive hibernating") {
      val values = for (i <- 1 to 100) yield (i, i)
      val instance = new MapReduceQuickSort[Int, Int, Int](values, identity, _ + _)

      instance.summaryBetween(1, 100) should equal(Some(5050))
      instance.hibernate()
      instance.summaryBetween(1, 100) should equal(Some(5050))
    }
  }
}