package uk.me.jamespic.dougng.model.datamanagement

import akka.actor.{Actor, ActorRef, Terminated, ActorLogging, Props}
import java.util.LinkedList
import java.util.Queue
import scala.collection.mutable.{Map => MMap}
import scala.concurrent.Future
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import scala.concurrent.Promise
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
    case Terminated(msg) => childTerminated
  }

  private def childTerminated = {
    forget(sender)
    maybeEndActivity
  }

  private def forget(actor: ActorRef) = {
    // If an actor is terminated, free it from any obligations
    dataDependentChildren -= actor
    remindMeLaters -= actor
    responseCounts -= actor
  }

  private def handleUpdates: Receive = {
    case upd: PleaseUpdate if Some(sender) == masterActivity =>
      messageAllChildren(upd)
    case AllDone =>
      dec(sender)
//      if (masterActivity == Some(sender)) doRemindLaters
    case RemindMeLater =>
      dec(sender)
      remindMeLaters += sender
  }

  private def inc(actor: ActorRef) = responseCounts(actor) += 1
  private def dec(actor: ActorRef) = {
    val prev = responseCounts(actor)
    responseCounts(actor) = prev - 1 max 0
    if (prev < 1) log.warning(s"Received a response from $actor, that doesn't correspond to a request")
  }

  private def messageAllChildren(msg: Any) = sendUpdate(dataDependentChildren &~ remindMeLaters, msg)
  private def doRemindLaters = {
    remindMeLaters foreach permitUpdate
    remindMeLaters = Set.empty
  }

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
      masterActivity = Some(nextOp)
      nextOp ! PleaseUpdate
      inc(nextOp)
      context become andBecome
      true
    } else false
  }

  private def permitUpdate(actor: ActorRef) = {
    actor ! PleaseUpdate
    inc(actor)
  }

  private def maybeEndActivity = {
    if (responseCounts.forall(_._2 == 0)) {
      doRemindLaters
    }
    // Check that responseCounts is still empty
    if (responseCounts.forall(_._2 == 0)) {
      cleanUpAfterActivity
      maybeStartActivity
    }
  }

  private def cleanUpAfterActivity = {
    responseCounts.clear
    remindMeLaters = Set.empty
    context become idle
    masterActivity = None
  }

  private def createDDActor(req: CreateDataDependentActor) = {
    val CreateDataDependentActor(cons, name) = req
    val actor = superviseNewActor(cons(pool), name)
    sender ! ActorCreated(actor, name)
    actor
  }

  private def getDataset(recordId: String) = {
    val name = DatasetName(recordId)
    val ds = datasets.getOrElse(recordId, {
      val actor = superviseNewActor(
          Props(new DatasetActor(recordId, dataFactory, pool)), name)
      datasets += recordId -> actor
      actor
    })
    sender ! ActorCreated(ds, name)
  }

  private def superviseNewActor(props: Props, name: String) = {
      val actor = context.actorOf(props, name)
    context.watch(actor)
    dataDependentChildren += actor
    actor
  }
}