package uk.me.jamespic.dougng.model

import org.scalatest._
import uk.me.jamespic.dougng.DBMixin
import java.util.Date
import scala.collection.JavaConversions._
import java.util.{List => JList, Set => JSet}

class SampleTest extends FunSpec with ShouldMatchers with DBMixin with GivenWhenThen {
  describe("The Sample singleton") {
    it("should create samples, with subsamples and counters") {
      import Sample._
      import util._

      Given("a sample")
      val sample = Sample("Parent Sample", new Date(0L))
      sample("responseTime") = 0.5

      And("a sub-sample")
      val child = Sample("Child Sample", new Date(1L), true, 200, "http://localhost")
      child("responseTime") = 0.4
      sample += child

      And("a counter")
      child.counters("CPU") = 50.0

      When("saved")
      sample.save()
      child.save()
      println(sample)
      println(sample("children"): Any)
      println(child)
      println((child("counters"): java.util.Set[_]).toList)

      Then("We should be find our records by querying")
      val results = docDb.asyncSql("""
          select parent.parent.name as gpName,
                 parent.parent.children as gpChildren,
                 parent.parent.responseTime as gpResponseTime,
                 parent.name as pName,
                 parent.counters as pCounters,
                 parent.responseTime as pResponseTime,
                 name
          from Counter
          """).toList
      results should have size(1)
      val result = results.head
      println(result)
      result[String]("name") should equal("CPU")
      result[String]("pName") should equal("Child Sample")
      result[String]("gpName") should equal("Parent Sample")
      result[Double]("gpResponseTime") should equal(0.5)
      result[Double]("pResponseTime") should equal(0.4)
      result[JSet[_]]("pCounters") should have size(1)
      result[JList[_]]("gpChildren") should have size(1)
      
      And("our indexes should contain everything we expect")
      val urlIndexed = docDb.asyncSql("""select from index:idx_Sample_url""").toList
      urlIndexed should have size(1)
      val nameIndexed = docDb.asyncSql("""select from index:idx_Sample_name""").toList
      nameIndexed should have size(2)
    }
  }
}