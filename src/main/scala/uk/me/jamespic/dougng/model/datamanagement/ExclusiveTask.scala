package uk.me.jamespic.dougng.model.datamanagement

import akka.actor.{Actor, ActorRef}
import akka.actor.actorRef2Scala

trait ExclusiveTask extends Actor {
  protected val database: ActorRef
  override def preStart = {
    super.preStart
    database ! RequestExclusiveDatabaseAccess
  }

  def doTask: Unit

  override def receive = {
    case ExclusiveAccessGranted => doTask
    case x: PleaseUpdate => sender ! AllDone
  }
}