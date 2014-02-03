package uk.me.jamespic.dougng.viewmodel

import org.scalatest.FunSpecLike
import uk.me.jamespic.dougng.model.RegisteringMixin
import org.scalatest.GivenWhenThen
import org.scalatest.Matchers
import akka.testkit.TestActorRef
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import akka.actor.Props
import uk.me.jamespic.dougng.model.datamanagement._
import java.util.Date
import uk.me.jamespic.dougng.model.TimeGraph
import uk.me.jamespic.dougng.model.Dataset
import scala.collection.JavaConversions._


class TimeGraphViewModelTest(_system: ActorSystem) extends TestKit(_system) with
	FunSpecLike with ImplicitSender with Matchers
	with RegisteringMixin with GivenWhenThen with BeforeAndAfterAll with BeforeAndAfter {
  describe("A TimeGraphViewModel") {
    it("should provide a subscribable view of Time Graph data") {
      val rid = doSetUp
      val db = system.actorOf(Props(new Database(dbUri)))

      db ! TimeGraphViewModel.constructMsg(rid)
      val ActorCreated(instance, name) = expectMsgClass(classOf[ActorCreated])

      instance ! Subscribe(None)
      val DataChanged(updateNo, data: TimeGraphViewModel#Table) = expectMsgClass(classOf[DataChanged])
      updateNo should equal(0L)
      data should have size(1)
      val testPageRow = data("TestPage")
      testPageRow should have size(60)
      val (firstTs, firstData) = testPageRow.head
      firstTs should equal((start, start + 1 * minute))
      firstData should be (500.0 +- 1.0)
      val (lastTs, lastData) = testPageRow.last
      lastTs should equal((start + 59 * minute, start + 60 * minute))
    }
  }

  val second = 1000
  val minute = 60 * 1000
  val start = 12L * 60L * 60L * 1000L + 3 * second
  def doSetUp = {
    import uk.me.jamespic.dougng.model.Sample._
    import uk.me.jamespic.dougng.model.util._
    // Create test result data
    // Want 10 requests per minute, for 1 hour, starting 12 hours after epoch 0
    val test = Test("My Test", new Date(start))
    test.save
    for (i <- 0 until 600) yield {
      val sample = Sample("TestPage", new Date(start + i * 6 * second), true, 200, "http://localhost")
      sample("responseTime") = 500
      sample.test = test
      sample.save
    }

    // Create Dataset
    var dataset = new Dataset
    dataset.metric = "responseTime"
    dataset.rowName = "name"
    dataset.table = "Sample"
    dataset.timestamp = "timestamp"
    dataset.whereClause = "responded = true"
    dataset = db.save(dataset)


    // Create TimeGraph
    var timegraph = new TimeGraph
    timegraph.granularity = minute
    timegraph.datasets = List(dataset)
    timegraph.name = "My Graph"
    timegraph = db.save(timegraph)
    timegraph.id
  }
}