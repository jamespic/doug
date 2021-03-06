package uk.me.jamespic.dougng.model.datamanagement

import akka.actor.{Actor, ActorRef, Terminated, ActorLogging, Props}
import java.util.LinkedList
import java.util.Queue

import com.orientechnologies.orient.core.command.OCommandOutputListener
import com.orientechnologies.orient.core.db.tool.ODatabaseImport

import scala.collection.mutable.{Map => MMap}
import scala.concurrent.Future
import com.orientechnologies.orient.`object`.db.{OObjectDatabasePool, OObjectDatabaseTx}
import scala.concurrent.Promise
import uk.me.jamespic.dougng.model.DatasetName
import scala.collection.JavaConversions._
import akka.actor.SupervisorStrategy
import uk.me.jamespic.dougng.model.util._
import java.io.InputStream

object Database {
  def dataFactory = DataStore.disk

  def createEmpty(url: String) = {
    val db = new OObjectDatabaseTx(url)
    db.create(): OObjectDatabaseTx
    initDB(db)
    try Props(classOf[Database], url)
    finally db.close()
  }

  def openExisting(url: String) = {
    val db = new OObjectDatabaseTx(url)
    db.open("admin","admin")
    registerClasses(db)
    try Props(classOf[Database], url)
    finally db.close()
  }

  def importData(url: String, input: InputStream) = {
    val db = new OObjectDatabaseTx(url)
    db.create(): OObjectDatabaseTx
    registerClasses(db)
    new ODatabaseImport(db.getUnderlying, input, new OCommandOutputListener {def onMessage(iText: String) = ()}).importDatabase()
    try Props(classOf[Database], url)
    finally db.close()
  }
}

class Database(url: String) extends Actor with ActorLogging {
  import Database._
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  private val pool = new OObjectDatabasePool(url, "admin", "admin")

  private val responseCounts = MMap.empty[ActorRef, Int].withDefault(_ => 0)
  private val bigOpQueue = new LinkedList[ActorRef]
  private val smallOpQueue = new LinkedList[ActorRef]
  private val readQueue = new LinkedList[ActorRef]
  private var masterActivity: Option[ActorRef] = None
  private var remindMeLaters = Set.empty[ActorRef]
  private var dataDependentChildren = Set.empty[ActorRef]
  private var datasets = Map.empty[String, ActorRef]

  def receive = idle
  private def standardBehaviour: Receive = {
    case req: CreateDataDependentActor => createDDActor(req)
    case GetDataset(recordId) => getDataset(recordId)
    case RequestPermissionToRead => readQueue push sender
    case RequestPermissionToUpdate => smallOpQueue push sender
    case RequestExclusiveDatabaseAccess => bigOpQueue push sender
    case Terminated(child) => childTerminated(child)
  }

  private def childTerminated(child: ActorRef) = {
    forget(child)
    maybeEndActivity
  }

  private def forget(actor: ActorRef) = {
    // If an actor is terminated, free it from any obligations
    dataDependentChildren -= actor
    remindMeLaters -= actor
    responseCounts -= actor
    actor.path.name match {
      case DatasetName(recordId) => datasets -= recordId
      case _ => // Do nothing
    }
  }

  private def handleUpdates: Receive = {
    case upd: ForwardableUpdateInfo if Some(sender) == masterActivity =>
      messageAllChildren(upd)
    case AllDone =>
      dec(sender)
    case RemindMeLater =>
      dec(sender)
      remindMeLaters += sender
  }

  private def postponeUpdates: Receive = {
    case upd: ForwardableUpdateInfo if Some(sender) == masterActivity =>
      // Postpone update until after task complete
      remindMeLaters ++= dataDependentChildren -- masterActivity
    case AllDone | RemindMeLater =>
      dec(sender)
  }

  private def expectNoUpdates: Receive = {
    case AllDone => dec(sender)
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
  private def reading = {
    (standardBehaviour orElse expectNoUpdates) andThen (_ => maybeRead) andThen canEndActivity
  }
  private def smallOp = {
    (standardBehaviour orElse handleUpdates) andThen canEndActivity
  }
  private def bigOp = {
    (standardBehaviour orElse postponeUpdates) andThen canEndActivity
  }

  private def canStartActivity: PartialFunction[Unit, Unit] = {case _ => maybeStartActivity}
  private def maybeStartActivity = {
    maybeRead || maybeSmallOp || maybeBigOp
  }

  def maybeRead: Boolean = {
    if (!readQueue.isEmpty) {
      readQueue foreach permitUpdate
      readQueue.clear
      context become reading
      true
    } else false
  }
  def maybeSmallOp = maybeNextInQueue(smallOpQueue, PleaseUpdate, smallOp)
  def maybeBigOp = maybeNextInQueue(bigOpQueue, ExclusiveAccessGranted, bigOp)

  private def canEndActivity: PartialFunction[Any, Unit] = {case _ => maybeEndActivity}
  private def maybeNextInQueue(q: Queue[ActorRef], message: PleaseUpdate, andBecome: Receive) = {
    val nextOp = q.poll
    if (nextOp != null) {
      masterActivity = Some(nextOp)
      nextOp ! message
      inc(nextOp)
      context become andBecome
      true
    } else false
  }

  private def permitUpdate(actor: ActorRef) = {
    actor ! PleaseRead
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
    val actor = superviseNewActor(cons(ConstructorInfo(pool, self)), name)
    sender ! ActorCreated(actor, name)
    actor
  }

  private def getDataset(recordId: String) = {
    val name = DatasetName(recordId)
    val ds = datasets.getOrElse(recordId, {
      val actor = superviseNewActor(
          Props(classOf[DatasetActor], recordId, dataFactory, pool, self), name)
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