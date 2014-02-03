package uk.me.jamespic.dougng.viewmodel

import org.scalatest.FunSpecLike
import org.scalatest.GivenWhenThen
import org.scalatest.Matchers
import akka.testkit.TestActorRef
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter

import akka.actor.Props
import akka.actor.Terminated
import akka.actor.Actor
import akka.actor.Kill


class SubscribableVariableTest(_system: ActorSystem) extends TestKit(_system) with
	FunSpecLike with ImplicitSender with Matchers
	with GivenWhenThen with BeforeAndAfterAll with BeforeAndAfter {

  def this() = this(ActorSystem("TestSystem"))

  describe("A SubscribableVariable") {
    it("should relay messages to subscribed actors") {
      val instance = system.actorOf(Props[TestSubscribableVariable])
      instance ! Emit("message 1")
      expectNoMsg

      instance ! Subscribe(None)
      expectMsg(DataChanged(0L, "message 1"))
      instance ! Emit("message 2")
      expectMsg(DataChanged(1L, "message 2"))

      instance ! Unsubscribe
      instance ! Emit("message 3")
      expectNoMsg
    }

    it("should detect termination of subscribed actors") {
      var outer = self
      val fakeActor = system.actorOf(Props(new Actor {
        def receive = {
          case _ => self ! "FAILED"
        }
      }))
      watch(fakeActor)

      val instance = system.actorOf(Props[TestSubscribableVariable])

      instance.tell(Subscribe(None), fakeActor)
      fakeActor ! Kill
      expectMsgClass(classOf[Terminated])

      instance ! Emit("message")
      expectNoMsg
    }

    it("should build new SubscribableVariabled with the monad") {
      import SubscribableVariable._
      val instance = system.actorOf(Props[TestSubscribableVariable])
      val adapter = instance map {
        case x: String => x.toLong
      }
      adapter ! Subscribe(None)
      instance ! Emit("12")
      expectMsg(DataChanged(0L, 12L))
    }

    it("should only emit an introductory event, if the new listener has not seen it") {
      val instance = system.actorOf(Props[TestSubscribableVariable])

      instance ! Emit("message")
      instance ! Subscribe(Some(0L))
      expectNoMsg

      instance ! Emit("message 2")
      expectMsg(DataChanged(1L, "message 2"))

      instance ! Unsubscribe
      instance ! Subscribe(Some(0L))
      expectMsg(DataChanged(1L, "message 2"))
    }
  }
}

class TestSubscribableVariable extends SubscribableVariable {
  override def receive = super.receive orElse {
    case Emit(x) => fireUpdated(x)
  }
}

case class Emit(x: Any)