package uk.me.jamespic.dougng.model.datamanagement

import java.nio.charset.StandardCharsets

import akka.actor.{Props, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{GivenWhenThen, Matchers, FunSpecLike}
import play.api.libs.iteratee.Iteratee
import uk.me.jamespic.dougng.model.RegisteringMixin
import scala.concurrent.duration._

import scala.concurrent.Await

class ExportTaskTest(_system: ActorSystem) extends TestKit(_system) with
    FunSpecLike with ImplicitSender with Matchers
    with RegisteringMixin with GivenWhenThen {
  def this() = this(ActorSystem("TestSystem"))

  describe("An ExportTask") {
    it("should export the database in json format") {
      val db = system.actorOf(Props(new Database(dbUri)))

      val resultFuture = ExportTask.export(db) |>>> Iteratee.consume()
      val result = Await.result(resultFuture, 120 seconds)
      val json = spray.json.JsonParser(new String(result, StandardCharsets.UTF_8))
      json.asJsObject.fields should contain key "schema"

      // Check database is still accepting read requests
      db ! RequestPermissionToRead
      expectMsg(5 seconds, PleaseRead)
      db ! AllDone
    }

  }
}
