package uk.me.jamespic.dougng.model.datamanagement

import akka.actor.{Actor, ActorRef, Terminated, ActorLogging, Props}
import java.util.LinkedList
import java.util.Queue
import scala.collection.mutable.{Map => MMap}
import scala.concurrent.Future
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import scala.concurrent.Promise
import akka.actor.Kill
import uk.me.jamespic.dougng.model.DatasetName

object Database {
  def dataFactory = DataStore.disk
}

class Database(url: String) extends Actor with ActorLogging {
  import Database._
  private val pool = new ReplacablePool
  pool.url = url

  private val responseCounts = MMap.empty[ActorRef, Int].withDefault(_ => 0)
  private val bigOpQueue = new LinkedList[ActorRef]
  private val smallOpQueue = new LinkedList[ActorRef]
  private var masterActivity: Option[ActorRef] = None
  private var remindMeLaters = Set.empty[ActorRef]
  private var dataDependentChildren = Set.empty[ActorRef]
  private var datasets = Map.empty[String, ActorRef]

  def receive = idle
  private def standardBehaviour: Receive = {
    case req: CreateDataDependentActor => createDDActor(req)
    case GetDataset(recordId) => getDataset(recordId)
    case RequestPermissionToUpdate => smallOpQueue push sender
    case RequestExclusiveDatabaseAccess => bigOpQueue push sender
    case Terminated(msg) =>
      dataDependentChildren -= sender
  }

  private def handleUpdates: Receive = {
    case upd: PleaseUpdate if Some(sender) == masterActivity =>
      messageAllChildren(upd)
    case AllDone =>
      dec(sender)
      if (masterActivity == Some(sender)) doRemindLaters
    case RemindMeLater =>
      dec(sender)
      remindMeLaters += sender
  }

  private def inc(actor: ActorRef) = responseCounts(actor) += 1
  private def dec(actor: ActorRef) = {
    val prev = responseCounts(actor)
    responseCounts(actor) = prev - 1
    if (prev < 1) log.warning(s"Received a response from $actor, that doesn't correspond to a request")
  }

  private def messageAllChildren(msg: Any) = sendUpdate(dataDependentChildren &~ remindMeLaters, msg)
  private def doRemindLaters = sendUpdate(remindMeLaters, PleaseUpdate)

  private def sendUpdate(recipients: Traversable[ActorRef], msg: Any) = {
    for {child <- recipients
           if Some(child) != masterActivity} {
        child ! msg
        inc(child)
      }
  }

  private def idle = standardBehaviour andThen canStartActivity
  private def smallOp = {
    (standardBehaviour orElse handleUpdates) andThen canEndActivity
  }
  private def bigOp = {
    (standardBehaviour orElse handleUpdates) andThen canEndActivity
  }

  private def canStartActivity: PartialFunction[Unit, Unit] = {case _ => maybeStartActivity}
  private def maybeStartActivity = {
    maybeSmallOp || maybeBigOp
  }

  def maybeSmallOp = maybeNextInQueue(smallOpQueue, smallOp)
  def maybeBigOp = maybeNextInQueue(bigOpQueue, bigOp)

  private def canEndActivity: PartialFunction[Unit, Unit] = {case _ => maybeEndActivity}
  private def maybeNextInQueue(q: Queue[ActorRef], andBecome: Receive) = {
    val nextOp = q.poll
    if (nextOp != null) {
      nextOp ! PleaseUpdate
      masterActivity = Some(nextOp)
      responseCounts(nextOp) += 1
      context become andBecome
      true
    } else false
  }

  private def maybeEndActivity = {
    if (responseCounts.forall(_._2 == 0)) {
      cleanUpAfterActivity
      maybeStartActivity
    }
  }

  private def cleanUpAfterActivity = {
    responseCounts.clear
    remindMeLaters = Set.empty
    context.unbecome()
    masterActivity = None
  }

  private def createDDActor(req: CreateDataDependentActor) = {
    val CreateDataDependentActor(cons, name) = req
    val actor = context.actorOf(cons(pool), name)
    dataDependentChildren += actor
    sender ! ActorCreated(actor, name)
    actor
  }

  private def getDataset(recordId: String) = {
    val name = DatasetName(recordId)
    val ds = datasets.getOrElse(recordId, {
      val actor = context.actorOf(
          Props(new DatasetActor(recordId, dataFactory, pool)), name)
      datasets += recordId -> actor
      dataDependentChildren += actor
      actor
    })
    sender ! ActorCreated(ds, name)
  }
}