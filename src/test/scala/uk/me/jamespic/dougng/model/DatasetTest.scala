package uk.me.jamespic.dougng.model

import org.scalatest.FunSpec
import uk.me.jamespic.dougng.OrientMixin
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.ShouldMatchers
import com.orientechnologies.orient.core.record.impl.ODocument
import uk.me.jamespic.dougng.model.util._
import java.util.Date

class DatasetTest extends FunSpec with ShouldMatchers with OrientMixin with GivenWhenThen {
  def createTestData() = {
    def newSimpleDoc(time: Long, value: Double) = {
      val doc:ODocument = docDb.newInstance("Sample")
      doc("timestamp") = new Date(time)
      doc("value") = value
      doc("name") = "Test"
      doc.save()
    }

    def newCounterDoc(time: Long, cpu: Double) = {
      val doc: ODocument = docDb.newInstance("Sample")
      doc("timestamp") = new Date(time)
      doc("name") = "ComplexTest"
      val counterDoc = new ODocument()
      counterDoc("cpu") = cpu
      doc("counters") = counterDoc
      doc.save()
    }

    def newDistraction(time: Long) = {
      val doc1: ODocument = docDb.newInstance("Sample")
      doc1("timestamp") = new Date(time)
      doc1("name") = "ComplexTest"
      doc1("counters") = new ODocument()
      doc1.save()

      val doc2: ODocument = docDb.newInstance("Sample")
      doc2("timestamp") = new Date(time)
      doc2("name") = "Test"
      doc2("counters") = new ODocument()
      doc2.save()
    }

    newSimpleDoc(0L, 1.0)
    newSimpleDoc(1000L, 2.0)
    newSimpleDoc(3000L, 6.0)
    newSimpleDoc(30000L, 10.0)
    newSimpleDoc(31000L, 11.0)
    newSimpleDoc(32000L, 12.0)

    newCounterDoc(60000L, 100.0)
    newCounterDoc(61000L, 98.0)
    newCounterDoc(90000L, 70.0)
    newCounterDoc(92000L, 80.0)

    newDistraction(5000L)
    newDistraction(35000L)
    newDistraction(62000L)
    newDistraction(99000L)
  }



  describe("A simple Dataset") {
    def simpleDataset = {
      createTestData()

      val ds = new Dataset
      ds.metric = "value"
      ds.rowName = "'Test Row'"
      ds.whereClause = "name = 'Test'"
      ds
    }

    it("should calculate simple windowed averages") {
      Given("a simple dataset")
      val ds = simpleDataset

      When("queried")
      val result = ds.avgData(docDb, 30000L)

      Then("there should be 2 results, with averages of 3.0 and 11.0")
      result should have size(1)
      val row = result("Test Row")
      row should have size(2)
      row(0L) should be (3.0 plusOrMinus 0.0001)
      row(30000L) should be (11.0 plusOrMinus 0.0001)
    }

    it("should calculate simple windowed maxima") {
      Given("a simple dataset")
      val ds = simpleDataset

      When("queried")
      val result = ds.maxData(docDb, 30000L)

      Then("there should be 2 results, with maxima of 6.0 and 12.0")
      result should have size(1)
      val row = result("Test Row")
      row should have size(2)
      row(0L) should be (6.0 plusOrMinus 0.0001)
      row(30000L) should be (12.0 plusOrMinus 0.0001)
    }
  }

  describe("A more complex Dataset") {
    def complexDataset = {
      createTestData()

      val ds = new Dataset
      ds.metric = "counters.cpu"
      ds.rowName = "'Test Row'"
      ds.whereClause = "name = 'ComplexTest'"
      ds
    }

    it("should calculate windowed averages") {
      Given("a dataset")
      val ds = complexDataset

      When("queried")
      val result = ds.avgData(docDb, 30000L)

      Then("there should be 2 results, with averages of 99.0 and 75.0")
      result should have size(1)
      val row = result("Test Row")
      row should have size(2)
      row(60000L) should be (99.0 plusOrMinus 0.0001)
      row(90000L) should be (75.0 plusOrMinus 0.0001)
    }

    it("should calculate windowed maxima") {
      Given("a dataset")
      val ds = complexDataset

      When("queried")
      val result = ds.maxData(docDb, 30000L)

      Then("there should be 2 results, with averages of 100.0 and 80.0")
      result should have size(1)
      val row = result("Test Row")
      row should have size(2)
      row(60000L) should be (100.0 plusOrMinus 0.0001)
      row(90000L) should be (80.0 plusOrMinus 0.0001)
    }
  }
}