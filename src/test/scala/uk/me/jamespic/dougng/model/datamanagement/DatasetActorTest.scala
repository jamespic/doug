package uk.me.jamespic.dougng.model.datamanagement

import org.scalatest.FunSpecLike
import uk.me.jamespic.dougng.OrientMixin
import org.scalatest.GivenWhenThen
import org.scalatest.Matchers
import akka.testkit.TestActorRef
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import java.util.Date
import uk.me.jamespic.dougng.model.util._
import akka.actor.Props
import scala.concurrent.duration._
import uk.me.jamespic.dougng.model.Sample._
import uk.me.jamespic.dougng.model.Dataset
import uk.me.jamespic.dougng.util.MappedAllocator
import uk.me.jamespic.dougng.model.RegisteringMixin

class DatasetActorTest(_system: ActorSystem) extends TestKit(_system) with
	FunSpecLike with ImplicitSender with Matchers
	with RegisteringMixin with GivenWhenThen with BeforeAndAfterAll with BeforeAndAfter {

  def this() = this(ActorSystem("TestSystem"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    super.afterAll
  }

  describe("A DatasetActor") {
	it("should allow us to query a simple dataset") {
	  // Create example dataset - doesn't need to be in the DB
	  var dataset = new Dataset
	  dataset.metric = "value"
	  dataset.rowName = "name"
	  dataset.whereClause = "name = 'MyRow'"
	  dataset = db.save(dataset)

	  // Put some simple data in the database
	  for (i <- 1L to 100L) {
	    val sample = Sample("MyRow", new Date(i))
	    sample("value") = i.toDouble
	    sample.save
	  }

	  val pool = new ReplacablePool
	  pool.url = dbUri

	  val instance = system.actorOf(Props(new DatasetActor(dataset.id, DataStore.memory, pool)))

	  // Initialise
	  instance ! PleaseUpdate
	  expectMsg(5 seconds, AllDone)

	  // Let's get some metadata
	  instance ! GetMetadata("req1")
	  expectMsg(5 seconds, Metadata(1L, 100L, Set("MyRow"), "req1"))

	  // And some data
	  instance ! GetAllSummaries(Seq((1L, 10L)), "req2")
	  val summary = expectMsgType[Summaries](5 seconds)
	  summary.corrId should equal("req2")
	  val rowMap = summary.result("MyRow").toMap
	  rowMap((1L, 10L)).get.getSum should be (55.0 +- 0.1)

	  // Some more data, just for fun
	  instance ! GetAllInRange(25L, 50L, "req3")
	  expectMsg(Ranges(Map("MyRow" -> (25L to 50L map (x => (x, x.toDouble)))), "req3"))

	  val newSample = for (db <- pool) yield {
	    val sample = Sample("MyRow", new Date(101L))
	    sample("value") = 101.0
	    sample.save
	    sample
	  }
	  instance ! ListenTo
	  instance ! DocumentsInserted(Seq(newSample))
	  expectMsgAllOf(5 seconds, AllDone, DataUpdatedNotification)

	  instance ! GetAllInRange(101L, 101L, "req4")
	  expectMsg(Ranges(Map("MyRow" -> Seq(101L -> 101.0)), "req4"))

	  instance ! PleaseUpdate
	  expectMsgAllOf(5 seconds, AllDone, DataUpdatedNotification)

	  instance ! UnlistenTo
	  instance ! PleaseUpdate
	  expectMsg(5 seconds, AllDone)
	  expectNoMsg(5 seconds)

	  instance ! GetAllInRange(101L, 101L, "req5")
	  expectMsg(Ranges(Map("MyRow" -> Seq(101L -> 101.0)), "req5"))
	}
  }
}