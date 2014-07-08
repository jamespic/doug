package uk.me.jamespic.dougng.viewmodel

import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.impl.ODocument
import org.scalatest.FunSpecLike
import uk.me.jamespic.dougng.model.{Dataset, RegisteringMixin}
import org.scalatest.GivenWhenThen
import org.scalatest.Matchers
import akka.actor.{Props, ActorSystem}
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import uk.me.jamespic.dougng.model.datamanagement._


class DatasetListViewModelTest(_system: ActorSystem) extends TestKit(_system) with
	FunSpecLike with ImplicitSender with Matchers
	with RegisteringMixin with GivenWhenThen with BeforeAndAfterAll with BeforeAndAfter {

  def this() = this(ActorSystem("TestSystem"))
  describe("A DatasetListViewModel") {
    it("should provide a subscribable list of all datasets") {
      val pool = new ReplacablePool
      pool.url = dbUri
      val instance = system.actorOf(Props(new DatasetListViewModel(pool, self)))

      expectMsg(RequestPermissionToRead)
      instance ! PleaseRead
      expectMsg(AllDone)

      instance ! Subscribe(None)
      expectMsg(DataChanged(0L, Map.empty))

      var ds1 = new Dataset
      ds1.name = "DS1"
      ds1.query = "Select from Sample"
      ds1 = db.save(ds1)
      val ds1id = ds1.id
      val doc1: ODocument = docDb.load(new ORecordId(ds1id))


      instance ! DocumentsInserted(Seq(doc1))
      expectMsg(AllDone)
      val dataUpdate = expectMsgClass(classOf[DataChanged])
      val data = dataUpdate.x.asInstanceOf[Map[String, Dataset]]
      data(ds1id).query should equal("Select from Sample")

      ds1.query = "Select * from Sample"
      ds1 = db.save(ds1)

      instance ! DatasetUpdate(Set(ds1id))

      expectMsg(AllDone)
      val dataUpdate2 = expectMsgClass(classOf[DataChanged])
      val data2 = dataUpdate2.x.asInstanceOf[Map[String, Dataset]]
      data2(ds1id).query should equal("Select * from Sample")

      db.delete(ds1)

      instance ! DocumentsDeleted(Set(ds1id))
      expectMsg(AllDone)
      expectMsg(DataChanged(3L, Map.empty))
    }
  }
}