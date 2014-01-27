package uk.me.jamespic.dougng.model

import org.scalatest.FunSpec
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.ShouldMatchers

class DatasetNameTest extends FunSpec with ShouldMatchers with GivenWhenThen{
  describe("The DatasetName object") {
    it("should format dataset names in an akka compatible way") {
      DatasetName("#12:25") should equal("dataset-12-25")
    }
    it("should unformat them to OrientDB compatible record ids") {
      DatasetName.unapply("dataset-12-25") should equal(Some("#12:25"))
      DatasetName.unapply("dataset-aa-bb") should equal(None)
    }
  }
}