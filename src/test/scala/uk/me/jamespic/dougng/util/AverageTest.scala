package uk.me.jamespic.dougng.util

import org.scalatest.FunSpecLike
import org.scalatest.Matchers
import org.scalatest.GivenWhenThen

class AverageTest extends FunSpecLike with Matchers with GivenWhenThen {
  describe("An Average") {
    it("should have an average of 0 when empty") {
      Given("an empty Average")
      val a = Average
      Then("its average should be 0")
      a.avg should be (0.0)
    }

    it("should average a short list of numbers") {
      Given("an empty Average")
      val a = Average
      When("we add the numbers 9, 9, and 12")
      val newAvg = a + 9 + 9 + 12
      Then("the average should be 10")
      newAvg.avg should be (10.0 +- 0.001)
    }
  }
}