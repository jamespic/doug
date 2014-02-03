package uk.me.jamespic.dougng.viewmodel

import akka.actor.{Actor, ActorRef, Terminated, Props}
import scala.reflect.ClassTag
import akka.actor.ActorContext
import akka.actor.ActorRefFactory

object SubscribableVariable {
  implicit class MapAdapter(delegate: ActorRef) {
    def map(f: PartialFunction[Any, Any])(implicit factory: ActorRefFactory) = {
      val newActor = factory.actorOf(Props(new Adapter(f, delegate)))
      delegate.tell(Subscribe(None), newActor)
      newActor
    }
  }
  class Adapter(f: PartialFunction[Any, Any], delegate: ActorRef) extends SubscribableVariable {
    override def receive = super.receive orElse {
      case DataChanged(updateId, x) => f.lift(x).foreach(fireUpdated)
    }
  }
}

trait SubscribableVariable extends Actor {
  private var listeners = Set.empty[ActorRef]
  private var lastUpdate: Option[DataChanged] = None
  def receive = {
    case Subscribe(updateId) => subscribe(updateId, sender)
    case Unsubscribe => unsubscribe(sender)
    case Terminated(ref) => unsubscribe(ref)
  }

  private def subscribe(updateId: Option[Long], ref: ActorRef) = {
    listeners += ref
    context.watch(ref)
    onSubscribe(ref)
    for (lastUpd @ DataChanged(lastId, data) <- lastUpdate;
         if updateId.forall(_ < lastId)) {
      ref ! lastUpd
    }
  }

  private def unsubscribe(ref: ActorRef) = {
	listeners -= ref
	onUnsubscribe(ref)
  }

  protected def onSubscribe(ref: ActorRef): Unit = {}
  protected def onUnsubscribe(ref: ActorRef): Unit = {}
  protected def fireUpdated(x: Any) = {
    val updateId = lastUpdate match {
      case Some(DataChanged(updId, _)) => updId + 1L
      case _ => 0L
    }
    val update = DataChanged(updateId, x)
    for (listener <- listeners) listener ! update
    lastUpdate = Some(update)
  }
}