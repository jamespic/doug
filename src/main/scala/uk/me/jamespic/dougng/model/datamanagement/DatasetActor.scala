package uk.me.jamespic.dougng.model.datamanagement

import java.lang
import java.util.{Date, Locale}

import akka.actor._
import com.orientechnologies.orient.core.command.OBasicCommandContext
import com.orientechnologies.orient.core.sql.{ORuntimeResult, OCommandExecutorSQLSelect, OCommandSQL}
import com.orientechnologies.orient.core.sql.filter.{OSQLTarget, OSQLFilter}
import scala.collection.JavaConversions._
import com.orientechnologies.orient.core.record.impl.ODocument
import uk.me.jamespic.dougng.model.Dataset
import scala.util.control.NonFatal
import scala.concurrent.duration._
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.common.exception.OException

object DatasetActor {
  private def notifyLaterTime = 1 second // FIXME: This should be configurable

  private case object NotificationTime

  private sealed trait NewDataResult
  private final case class ShouldInsert(data: Traversable[ODocument]) extends NewDataResult
  private case object CantHandleInsertion extends NewDataResult
  private trait DataInsertionHandler {
    def apply(docs: Traversable[ODocument]): NewDataResult
  }
  private object SimpleHandler extends DataInsertionHandler {
    def apply(docs: Traversable[ODocument]) = CantHandleInsertion
  }

  private val NameField = "name"
  private val TimestampField = "timestamp"
  private val ValueField = "value"
  private val neededProjections = Seq(TimestampField, NameField, ValueField)
  val SelectQueryRegex = """(?i)SELECT\s+(.+)\s+FROM\s+(.+)\s+WHERE\s+(.+)""".r
  private object Timestamp {
    def unapply(x: Any) = x match {
      case l: Number => Some(l.longValue)
      case d: Date => Some(d.getTime)
      case _ => None
    }
  }

  def constructor(datasetId: String, dataFactory: => DataStore)(consInfo: ConstructorInfo) = {
    new DatasetActor(datasetId, dataFactory, consInfo.pool, consInfo.database)
  }
}

class DatasetActor(private var datasetId: String,
    dataFactory: => DataStore,
    pool: ReplacablePool,
    protected val database: ActorRef)
    extends Actor with Stash with RequestReadOnStart with ActorLogging {
  import DatasetActor._
  private var dataOpt: Option[DataStore] = None
  private var listeners = Set.empty[ActorRef]
  private var notificationPending = false
  private var dataset: Dataset = null

  private var insertHandler: DataInsertionHandler = SimpleHandler
  private var lastError: Option[Throwable] = None

  private def updateRecord(db: OObjectDatabaseTx) {
    dataset = db.load(new ORecordId(datasetId))
  }

  private def updateInsertHandler {
    // Special-case selection from classes, so we can handle inserts deterministically
    insertHandler = try {
      val command = new OCommandSQL(dataset.query)
      val executor = new OCommandExecutorSQLSelect() // Only used for its parsing capabilities
      executor.parse(command)
      val projections = executor.getProjections
      if ((projections == null)
          || executor.isAnyFunctionAggregates
          || !projections.keySet.containsAll(neededProjections)) {
        SimpleHandler // Can't update incrementally if projections are aggregates or grouped
      } else {
        dataset.query match {
          case SelectQueryRegex(_, table, whereClause) =>
            val target = new OSQLTarget(table, null, null)
            val classes = target.getTargetClasses.keySet.toSet
            val filter = new OSQLFilter(whereClause, null, null)
            if (classes != null) {
              new DataInsertionHandler {
                def apply(docs: Traversable[ODocument]): NewDataResult = {
                  var i = 0
                  val context = new OBasicCommandContext
                  val insertableRows = for (doc: ODocument <- docs
                       if classes.exists(_.isSuperClassOf(doc.getSchemaClass))
                       && filter.evaluate(doc, doc, context) == java.lang.Boolean.TRUE) yield {
                    i += 1
                    context.setVariable("current", doc) // Emulate similar behaviour in OCommandExecutorSQLSelect
                    val projection = ORuntimeResult.getProjectionResult(i, projections, context, doc)
                    projection
                  }
                  ShouldInsert(insertableRows)
                }
              }
            } else SimpleHandler
        }
      }
    } catch {
      case NonFatal(_) => SimpleHandler // If we fail, for any reason, no class browsing
    }
  }

  private def insertDocuments(projections: Traversable[ODocument]) = {
    for (projection <- projections;
         rowName <- Option(projection.field[Any](NameField));
         Timestamp(timestamp) <- Option(projection.field[Any](TimestampField));
         value <- Option(projection.field[Any](ValueField)) if value.isInstanceOf[Number]) {
      data(rowName.toString) += (timestamp -> value.asInstanceOf[Number].doubleValue)
    }
  }

  override def postStop = {
    invalidate
    super.postStop
  }

  def receive = uninitialised

  def uninitialised: Receive = base orElse {
    case _ => stash()
  }

  def initialised: Receive = base orElse {
    /*
     * Update notification/permission messages
     */
    case ListenTo => listenTo
    case UnlistenTo => listeners -= sender
    case GetMetadata(corrId) => sender ! Metadata(data.min, data.max, data.rows, corrId)
    case GetSummaries(rows, ranges, corrId) => summaries(rows, ranges, corrId)
    case GetAllSummaries(ranges, corrId) => summaries(data.rows, ranges, corrId)
    case GetInRange(rows, from, to, corrId) => getInRange(rows, from, to, corrId)
    case GetAllInRange(from, to, corrId) => getInRange(data.rows, from, to, corrId)
    case NotificationTime => notifyListeners
    case GetLastError(corrId) => LastError(lastError, corrId)
    case Terminated(listener) => listeners -= listener
  }

  def base: Receive = {
    case PleaseRead => initialise
    case DocumentsInserted(docs) => handleInsert(docs)
    case DatasetUpdate(ids) if ids contains dataset.id => initialise
    // Default, for an update message we don't care about
    case DocumentsDeleted(docs) if docs contains dataset.id => context.stop(self)
    case _: PleaseUpdate => sender ! AllDone
  }

  private def notifyListeners {
    notificationPending = false
    for (listener <- listeners) listener ! DataUpdatedNotification
  }

  private def notifyListenersLater {
    if (!notificationPending) {
      val system = context.system
      implicit val dispatcher = system.dispatcher
      system.scheduler.scheduleOnce(notifyLaterTime, self, NotificationTime)
      notificationPending = true
    }
  }

  private def listenTo {
    listeners += sender
    context.watch(sender)
    if (dataOpt.isDefined) sender ! DataUpdatedNotification
  }

  private def summaries(rows: Iterable[String], ranges: Iterable[(Long,Long)], corrId: Any) {
    val result = for (row <- rows) yield {
      val mrData = data(row)
      val rowData = for (r @ (low, high) <- ranges) yield {
        r -> mrData.summaryBetween(low, high)
      }
      row -> rowData.toMap
    }
    sender ! Summaries(result.toMap, corrId)
  }

  private def getInRange(rows: Iterable[String], from: Long, to: Long, corrId: Any) {
    val result = for (row <- rows) yield row -> data(row).getBetween(from, to).toSeq
    sender ! Ranges(result.toMap, corrId)
  }

  private def data = {
    dataOpt getOrElse renew
  }

  private def renew = {
    val data = dataFactory
    dataOpt = Some(data)
    data
  }

  private def regenerate {
    invalidate
    renew
    for (db <- pool) catchingOException(()) {
        import uk.me.jamespic.dougng.model.util.ObjectDBPimp
        updateRecord(db)
        updateInsertHandler
        insertDocuments(db.getUnderlying.asyncSql(dataset.query))
    }
    notifyListeners
  }

  private def handleInsert(docs: Traversable[ODocument]) {
    if (dataOpt == None) initialise
    else {
      for (db <- pool) {
        insertHandler(docs) match {
          case CantHandleInsertion => sender ! RemindMeLater
          case ShouldInsert(newDocs) =>
            insertDocuments(newDocs)
            notifyListenersLater
            sender ! AllDone
        }
      }
    }
  }

  private def initialise {
    regenerate
    context.become(initialised)
    unstashAll()
    sender ! AllDone
  }

  private def invalidate {
    dataOpt.foreach (_.close)
    dataOpt = None
  }

  private def catchingOException[X](backup: => X)(f: => X): X = {
    try f
    catch {
      case ex: OException =>
        backup
    }
  }
}