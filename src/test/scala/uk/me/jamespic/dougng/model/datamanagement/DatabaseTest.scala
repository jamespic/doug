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

class DatabaseTest(_system: ActorSystem) extends TestKit(_system) with
	FunSpecLike with ImplicitSender with Matchers
	with RegisteringMixin with GivenWhenThen with BeforeAndAfterAll with BeforeAndAfter {

  val timeout = 5 seconds
  def this() = this(ActorSystem("TestSystem"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    super.afterAll
  }

  describe("A Database") {
	it("should co-operate with an actor looking to use the database") {
	  val instance = system.actorOf(Props(new Database(dbUri)))

	  instance ! RequestPermissionToUpdate
	  expectMsg(timeout, PleaseUpdate)

	  var dataset = new Dataset
	  dataset.metric = "value"
	  dataset.rowName = "name"
	  dataset.whereClause = "name = 'MyRow'"
	  dataset = db.save(dataset)

	  instance ! PleaseUpdate
	  instance ! AllDone
	  expectNoMsg(5 seconds)

	  instance ! GetDataset(dataset.id)
	  val ActorCreated(dsActor, name) = expectMsgClass(timeout, classOf[ActorCreated])

	  dsActor ! ListenTo
	  expectMsg(timeout, DataUpdatedNotification)

	  instance ! RequestPermissionToUpdate
	  expectMsg(timeout, PleaseUpdate)

	  // Put some simple data in the database
	  val samples = for (i <- 1L to 100L) yield {
	    val sample = Sample("MyRow", new Date(i))
	    sample("value") = i.toDouble
	    sample.save
	    sample
	  }

	  instance ! DocumentsInserted(samples)
	  instance ! AllDone
	  expectMsg(DataUpdatedNotification)

	  dsActor ! GetAllSummaries(Seq((1L, 10L)))
	  val summary = expectMsgType[Summaries](timeout)
	  val rowMap = summary.result("MyRow").toMap
	  rowMap((1L, 10L)).get.getSum should be (55.0 +- 0.1)

	  expectNoMsg(5 seconds)
	}
  }
}