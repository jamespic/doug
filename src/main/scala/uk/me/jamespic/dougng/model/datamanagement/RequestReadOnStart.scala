package uk.me.jamespic.dougng.model.datamanagement

import akka.actor.{Actor, ActorRef}
import akka.actor.actorRef2Scala

trait RequestReadOnStart extends Actor {
  protected val database: ActorRef
  override def preStart = {
    super.preStart
    database ! RequestPermissionToRead
  }
}