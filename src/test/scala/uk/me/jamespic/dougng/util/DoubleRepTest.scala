package uk.me.jamespic.dougng.util

import org.scalatest.FunSpecLike
import org.scalatest.Matchers
import org.scalatest.GivenWhenThen

class DoubleRepTest extends FunSpecLike with Matchers with GivenWhenThen {
  describe("A DoubleRep") {
    it("should convert back to itself") {
      DoubleRep(1.0).toDouble should equal(1.0)
    }
    it("should convert back to itself with negative numbers") {
      DoubleRep(-1.0).toDouble should equal(-1.0)
    }
    it("should convert back to itself with zero") {
      DoubleRep(0.0).toDouble should equal(0.0)
    }
  }
}