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
import com.orientechnologies.orient.core.record.impl.ODocument
import akka.actor.PoisonPill


class GraphListViewModelTest(_system: ActorSystem) extends TestKit(_system) with
	FunSpecLike with ImplicitSender with Matchers
	with RegisteringMixin with GivenWhenThen with BeforeAndAfterAll with BeforeAndAfter {

  def this() = this(ActorSystem("TestSystem"))

  describe("A GraphListViewModel") {
    it("should provide a subscribable list of all Graphs") {
      val rid = createDummyTimeGraph("Graph1")
      val pool = new ReplacablePool
      pool.url = dbUri
      val instance = system.actorOf(Props(new GraphListViewModel(dummyFactory, pool, self)))
      expectMsg(RequestPermissionToRead)
      instance ! PleaseRead
      
      val CreateDataDependentActor(factory, name) = expectMsgClass(classOf[CreateDataDependentActor])
      expectMsg(AllDone)
      instance ! ActorCreated(system.actorOf(factory(null), name), name)

      instance ! Subscribe(None)
      val dataUpdate = expectMsgClass(classOf[DataChanged])
      dataUpdate.updateNo should equal(0L)
      val data = dataUpdate.x.asInstanceOf[Map[String, GraphInfo]]
      val GraphInfo(record, dummyActorRef) = data(rid)
      record.name should equal("Graph1")

      dummyActorRef ! "ping"
      expectMsg("ping")

      val rid2 = createDummyTimeGraph("Graph2")

      instance ! PleaseRead
      val CreateDataDependentActor(factory2, name2) = expectMsgClass(classOf[CreateDataDependentActor])
      expectMsg(AllDone)
      instance ! ActorCreated(system.actorOf(factory2(null), name2), name2)

      val dataUpdate2 = expectMsgClass(classOf[DataChanged])
      dataUpdate2.updateNo should equal(1L)
      val data2 = dataUpdate2.x.asInstanceOf[Map[String, GraphInfo]]
      val GraphInfo(record2, dummyActorRef2) = data2(rid2)
      record2.name should equal("Graph2")
      dummyActorRef2 should not equal(dummyActorRef)
      dummyActorRef2 ! "pong"
      expectMsg("pong")

      val graph2 = db.load[TimeGraph](new ORecordId(rid2))
      graph2.name = "Graph3"
      db.save(graph2)

      instance ! DatasetUpdate(Set(rid2))
      expectMsg(AllDone)

      val dataUpdate3 = expectMsgClass(classOf[DataChanged])
      dataUpdate3.updateNo should equal(2L)
      val data3 = dataUpdate3.x.asInstanceOf[Map[String, GraphInfo]]
      val GraphInfo(record3, dummyActorRef3) = data3(rid2)
      record3.name should equal("Graph3")
      dummyActorRef2 should equal(dummyActorRef3)

      db.delete(new ORecordId(rid))

      instance ! DocumentsDeleted(Set(rid))
      expectMsg(AllDone)

      val dataUpdate4 = expectMsgClass(classOf[DataChanged])
      dataUpdate4.updateNo should equal(3L)
      val data4 = dataUpdate4.x.asInstanceOf[Map[String, GraphInfo]]
      data4 should not contain key(rid)
      data4 should contain key(rid2)
      
      val rid3 = createDummyTimeGraph("Graph3")
      val doc = docDb.getRecord[ODocument](new ORecordId(rid3))
      
      instance ! DocumentsInserted(Set(doc))
      val CreateDataDependentActor(factory3, name3) = expectMsgClass(classOf[CreateDataDependentActor])
      expectMsg(AllDone)
      instance ! ActorCreated(system.actorOf(factory3(null), name3), name3)
      
      val dataUpdate5 = expectMsgClass(classOf[DataChanged])
      dataUpdate5.updateNo should equal(4L)
      val data5 = dataUpdate5.x.asInstanceOf[Map[String, GraphInfo]]
      data5 should contain key(rid3)
    }
    
    it("should handle actor failures, by requesting updates") {
      val rid = createDummyTimeGraph("Graph1")
      val pool = new ReplacablePool
      pool.url = dbUri
      val instance = system.actorOf(Props(new GraphListViewModel(dummyFactory, pool, self)))
      expectMsg(RequestPermissionToRead)
      instance ! PleaseRead
      
      val CreateDataDependentActor(factory, name) = expectMsgClass(classOf[CreateDataDependentActor])
      expectMsg(AllDone)
      val newActor = system.actorOf(factory(null), name)
      instance ! ActorCreated(newActor, name)

      newActor ! PoisonPill
      
      expectMsg(RequestPermissionToRead)
      instance ! PleaseRead
      val CreateDataDependentActor(factory2, name2) = expectMsgClass(classOf[CreateDataDependentActor])
      expectMsg(AllDone)
      val newActor2 = system.actorOf(factory2(null), name2)
      instance ! ActorCreated(newActor2, name2)

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