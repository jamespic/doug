package uk.me.jamespic.dougng.viewmodel

import akka.actor.{Actor, ActorRef, Terminated, Props}
import scala.reflect.ClassTag
import akka.actor.ActorContext
import akka.actor.ActorRefFactory

object SubscribableVariable {
  implicit class MapAdapter(delegate: ActorRef) {
    def map(f: PartialFunction[Any, Any])(implicit factory: ActorRefFactory) = {
      val newActor = factory.actorOf(Props(new Adapter(f, delegate)))
      delegate.tell(Subscribe, newActor)
      newActor
    }
  }
  class Adapter(f: PartialFunction[Any, Any], delegate: ActorRef) extends SubscribableVariable {
    override def receive = super.receive orElse {
      case DataChanged(x) => f.lift(x).foreach(fireUpdated)
    }
  }
}

trait SubscribableVariable extends Actor {
  private var listeners = Set.empty[ActorRef]
  def receive = {
    case Subscribe => subscribe(sender)
    case Unsubscribe => unsubscribe(sender)
    case Terminated(ref) => unsubscribe(ref)
  }

  private def subscribe(ref: ActorRef) = {
    listeners += ref
    context.watch(ref)
    onSubscribe(ref)
  }

  private def unsubscribe(ref: ActorRef) = {
	listeners -= ref
	onUnsubscribe(ref)
  }

  protected def onSubscribe(ref: ActorRef): Unit = {}
  protected def onUnsubscribe(ref: ActorRef): Unit = {}
  protected def fireUpdated(x: Any) = {
    val update = DataChanged(x)
    for (listener <- listeners) listener ! update
  }
}