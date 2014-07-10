package uk.me.jamespic.dougng.model.datamanagement

import java.nio.charset.StandardCharsets

import akka.actor.{Terminated, Props, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{GivenWhenThen, Matchers, FunSpecLike}
import play.api.libs.iteratee.Iteratee
import uk.me.jamespic.dougng.model.RegisteringMixin
import scala.concurrent.duration._

import scala.concurrent.Await

class CloseTaskTest(_system: ActorSystem) extends TestKit(_system) with
    FunSpecLike with ImplicitSender with Matchers
    with RegisteringMixin with GivenWhenThen {
  def this() = this(ActorSystem("TestSystem"))

  describe("A CloseTask") {
    it("should shut down the database") {
      val db = system.actorOf(Props(new Database(dbUri)))
      watch(db)

      val closeFuture = CloseTask.closeDB(db)
      val result = Await.result(closeFuture, 5 seconds)
      val termMsg = expectMsgClass(classOf[Terminated])
      termMsg.actor should equal(db)
    }

  }
}
