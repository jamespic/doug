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
import akka.actor.Actor
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.concurrent.Eventually
import akka.actor.PoisonPill
import akka.actor.ActorDSL
import java.util.concurrent.TimeoutException


class DatabaseTest(_system: ActorSystem) extends TestKit(_system) with
	FunSpecLike with ImplicitSender with Matchers
	with RegisteringMixin with GivenWhenThen with BeforeAndAfterAll with BeforeAndAfter
	with Eventually {

  val timeout = 5 days
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
	  expectMsg(timeout, DataUpdatedNotification)

	  dsActor ! GetAllSummaries(Seq((1L, 10L)))
	  val summary = expectMsgType[Summaries](timeout)
	  // FIXME FIXME FIXME - probable race condition means MyRow sometimes doesn't exist
	  val rowMap = summary.result("MyRow").toMap
	  rowMap((1L, 10L)).get.getSum should be (55.0 +- 0.1)

	  expectNoMsg(5 seconds)
	}

	it("should supervise data dependent actors") {
	  val instance = system.actorOf(Props(new Database(dbUri)))

	  val responseCount = new AtomicInteger(0)

	  def constructor(info: ConstructorInfo) = Props(new Actor {
	    override def preStart = {
	      super.preStart
	      info.database ! RequestPermissionToUpdate
	    }
	    def receive = {
	      case _: PleaseUpdate =>
	        responseCount.incrementAndGet()
	        info.database ! AllDone
	    }
	  })

	  instance ! CreateDataDependentActor(constructor, "MyActor")
	  val testActor = expectMsgClass(classOf[ActorCreated])
	  eventually {responseCount.intValue should equal(1)}

	  instance ! RequestPermissionToUpdate
	  expectMsg(PleaseUpdate)
	  instance ! DocumentsInserted(Set())
	  instance ! AllDone
	  eventually {responseCount.intValue should equal(2)}

	  // Test that actor termination is as good as AllDone
	  instance ! CreateDataDependentActor((pool => Props[NoopActor]), "BadActor")
	  val ActorCreated(badActor, name) = expectMsgClass(classOf[ActorCreated])

	  instance ! RequestPermissionToUpdate
	  expectMsg(PleaseUpdate)
	  // Send PleaseUpdate - this should be forwarded to everything, including badActor, which won't respond
	  instance ! PleaseUpdate
	  badActor ! PoisonPill
	  instance ! AllDone
	  eventually {responseCount.intValue should equal(3)}

	  // Check that the update session has finished, by requesting another
	  instance ! RequestPermissionToUpdate
	  expectMsg(PleaseUpdate)
	  instance ! AllDone
	}

	it("should allow concurrent reads, but not concurrent updates") {
  	  val instance = system.actorOf(Props(new Database(dbUri)))
  	  val inbox1 = ActorDSL.inbox()
  	  val inbox2 = ActorDSL.inbox()

  	  instance.tell(RequestPermissionToRead, inbox1.getRef)
  	  instance.tell(RequestPermissionToRead, inbox2.getRef)

  	  inbox1.receive(3 seconds) should equal(PleaseRead)
  	  inbox2.receive(3 seconds) should equal(PleaseRead)

  	  instance.tell(AllDone, inbox1.getRef)
  	  instance.tell(AllDone, inbox2.getRef)

  	  instance.tell(RequestPermissionToUpdate, inbox1.getRef)
  	  instance.tell(RequestPermissionToUpdate, inbox2.getRef)

  	  inbox1.receive(3 seconds) should equal(PleaseUpdate)
  	  intercept[TimeoutException] {
  	    inbox2.receive(3 seconds)
  	  }

  	  instance.tell(AllDone, inbox1.getRef)
  	  inbox2.receive(3 seconds) should equal(PleaseUpdate)
  	  instance.tell(AllDone, inbox2.getRef)
	}
  }
}

class NoopActor extends Actor {
  def receive = {case _ => /* Do Nothing */}
}