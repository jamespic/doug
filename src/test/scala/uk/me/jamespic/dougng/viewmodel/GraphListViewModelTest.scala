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
import uk.me.jamespic.dougng.model.TimeGraph
import uk.me.jamespic.dougng.model.datamanagement._
import akka.actor.Actor
import uk.me.jamespic.dougng.viewmodel.GraphListViewModel.GraphInfo
import java.util.UUID
import com.orientechnologies.orient.core.id.ORecordId
import scala.concurrent.duration._


class GraphListViewModelTest(_system: ActorSystem) extends TestKit(_system) with
	FunSpecLike with ImplicitSender with Matchers
	with RegisteringMixin with GivenWhenThen with BeforeAndAfterAll with BeforeAndAfter {

  def this() = this(ActorSystem("TestSystem"))

  describe("A GraphListViewModel") {
    it("should provide a subscribable list of all Graphs") {
      val rid = createDummyTimeGraph("Graph1")
      val database = system.actorOf(Props(new Database(dbUri)))
      database ! GraphListViewModel.constructMsg(dummyFactory)
      val ActorCreated(instance, _) = expectMsgClass(classOf[ActorCreated])

      instance ! Subscribe(None)
      val dataUpdate = expectMsgClass(classOf[DataChanged])
      dataUpdate.updateNo should equal(0L)
      val data = dataUpdate.x.asInstanceOf[Map[String, GraphInfo]]
      val GraphInfo(record, dummyActorRef) = data(rid)
      record.name should equal("Graph1")

      dummyActorRef ! "ping"
      expectMsg("ping")

      database ! RequestPermissionToUpdate
      expectMsg(PleaseUpdate)

      val rid2 = createDummyTimeGraph("Graph2")

      database ! PleaseRead
      database ! AllDone

      val dataUpdate2 = expectMsgClass(classOf[DataChanged])
      dataUpdate2.updateNo should equal(1L)
      val data2 = dataUpdate2.x.asInstanceOf[Map[String, GraphInfo]]
      val GraphInfo(record2, dummyActorRef2) = data2(rid2)
      record2.name should equal("Graph2")
      dummyActorRef2 should not equal(dummyActorRef)
      dummyActorRef2 ! "pong"
      expectMsg("pong")

      database ! RequestPermissionToUpdate
      expectMsg(PleaseUpdate)

      val graph2 = db.load[TimeGraph](new ORecordId(rid2))
      graph2.name = "Graph3"
      db.save(graph2)

      database ! DatasetUpdate(Set(rid2))
      database ! AllDone

      val dataUpdate3 = expectMsgClass(classOf[DataChanged])
      dataUpdate3.updateNo should equal(2L)
      val data3 = dataUpdate3.x.asInstanceOf[Map[String, GraphInfo]]
      val GraphInfo(record3, dummyActorRef3) = data3(rid2)
      record3.name should equal("Graph3")
      dummyActorRef2 should equal(dummyActorRef3)

      database ! RequestPermissionToUpdate
      expectMsg(PleaseUpdate)

      db.delete(new ORecordId(rid))

      database ! DocumentsDeleted(Set(rid))
      database ! AllDone

      val dataUpdate4 = expectMsgClass(classOf[DataChanged])
      dataUpdate4.updateNo should equal(3L)
      val data4 = dataUpdate4.x.asInstanceOf[Map[String, GraphInfo]]
      data4 should not contain key(rid)
      data4 should contain key(rid2)

    }
  }

  def createDummyTimeGraph(name: String) = {
    var ds = new TimeGraph
    ds.name = name
    ds = db.save[TimeGraph](ds)
    ds.id
  }

  val dummyFactory: GraphListViewModel.ViewModelFactory = {
    case x: TimeGraph => CreateDataDependentActor(pool => Props(new Actor {
      def receive = {
        // Dummy actor - doesn't actually use the pool, just echoes requests, and acknowledges updates
        case _: PleaseUpdate => sender ! AllDone
        case x => sender ! x
      }
    }), s"dummy-${UUID.randomUUID}")
  }
}