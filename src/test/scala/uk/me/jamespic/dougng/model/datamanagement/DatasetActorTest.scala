package uk.me.jamespic.dougng.model.datamanagement

import org.scalatest.FunSpec
import uk.me.jamespic.dougng.OrientMixin
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.ShouldMatchers
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
	FunSpec with ImplicitSender with ShouldMatchers
	with RegisteringMixin with GivenWhenThen with BeforeAndAfterAll with BeforeAndAfter {

  def this() = this(ActorSystem("TestSystem"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    super.afterAll
  }

  describe("A DatasetActor") {
	it("should allow us to query a simple dataset") {
	  // Create example dataset - doesn't need to be in the DB
	  val dataset = new Dataset
	  dataset.metric = "value"
	  dataset.rowName = "name"
	  dataset.whereClause = "name = 'MyRow'"

	  // Put some simple data in the database
	  for (i <- 1L to 100L) {
	    val sample = Sample("MyRow", new Date(i))
	    sample("value") = i.toDouble
	    sample.save
	  }

	  val pool = new ReplacablePool
	  pool.url = dbUri

	  val instance = system.actorOf(Props(new DatasetActor(dataset, DataStore.memory, pool)))

	  // Pre-initialisation, it doesn't know what tables it wants
	  instance ! WhichTablesAreYouInterestedIn
	  expectMsg(5 seconds, AllOfThem)

	  // Initialise
	  instance ! PermissionToUpdate
	  expectMsg(5 seconds, AllDone)

	  // It now knows what tables it expects
	  instance ! WhichTablesAreYouInterestedIn
	  expectMsg(5 seconds, TheseTables(Set("SAMPLE")))

	  // Let's get some metadata
	  instance ! GetMetadata
	  expectMsg(5 seconds, Metadata(1L, 100L, Set("MyRow")))

	  // And some data
	  instance ! GetAllSummaries(Seq((1L, 10L)))
	  val summary = expectMsgType[Summaries](5 seconds)
	  val rowMap = summary.result("MyRow").toMap
	  rowMap((1L, 10L)).get.getSum should be (55.0 plusOrMinus 0.1)

	  // Some more data, just for fun
	  instance ! GetAllInRange(25L, 50L)
	  expectMsg(Ranges(Map("MyRow" -> (25L to 50L map (x => (x, x.toDouble))))))

	  val newSample = Sample("MyRow", new Date(101L))
	  newSample("value") = 101.0
	  instance ! ListenTo
	  instance ! DocumentInserted(newSample)
	  //Thread.sleep(86400000L)
	  expectMsgAllOf(5 seconds, AllDone, DataUpdatedNotification)

	  instance ! GetAllInRange(101L, 101L)
	  expectMsg(Ranges(Map("MyRow" -> Seq(101L -> 101.0))))
	}
  }
}