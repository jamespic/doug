package uk.me.jamespic.dougng.model.util

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen
import uk.me.jamespic.dougng.util.MapReduceQuickSort

class MapReduceQuickSortTest extends FunSpec with ShouldMatchers with GivenWhenThen {
  describe("A MapReduceQuickSort") {
    describe("Containing the integers between 1 and 10000") {
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
  }
}