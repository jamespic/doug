package uk.me.jamespic.dougng.model.datamanagement

import akka.actor.{Actor, ActorRef, Terminated}
import com.orientechnologies.orient.core.command.{OBasicCommandContext,OCommandContext}
import com.orientechnologies.orient.core.sql.filter.{OSQLTarget, OSQLFilter}
import scala.collection.JavaConversions._
import com.orientechnologies.orient.core.record.impl.ODocument
import scala.collection.mutable.{Map => MMap}
import uk.me.jamespic.dougng.util.MutableMapReduce
import uk.me.jamespic.dougng.util.DetailedStats
import uk.me.jamespic.dougng.model.Dataset
import scala.util.control.NonFatal
import scala.concurrent.duration._
import resource._
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.exception.OQueryParsingException
import com.orientechnologies.common.exception.OException

object DatasetActor {
  object ImmutableContext extends OBasicCommandContext {
    variables = java.util.Collections.emptyMap[String, Object]
  }

  private def compile(cmd: String, ctx: OCommandContext) = new OSQLFilter(cmd, ctx, null)

  private def notifyLaterTime = 1 second // FIXME: This should be configurable

  private case object NotificationTime

  def constructor(datasetId: String, dataFactory: => DataStore)(pool: ReplacablePool) = {
    new DatasetActor(datasetId, dataFactory, pool)
  }
}

class DatasetActor(private var datasetId: String, dataFactory: => DataStore, pool: ReplacablePool) extends Actor {
  import DatasetActor._
  private var dataOpt: Option[DataStore] = None
  private var listeners = Set.empty[ActorRef]
  private var notificationPending = false
  private var dataset: Dataset = null

  private var classInsertHandler: Option[ClassInsertHandler] = None
  private var lastError: Option[Throwable] = None

  private def updateRecord(db: OObjectDatabaseTx) {
    dataset = db.load(new ORecordId(datasetId))
  }

  private def updateClassInsertHandler {
    classInsertHandler = try {
      val parsed = new OSQLTarget(dataset.table, null, null)
      if (parsed.getTargetClasses() != null) {
        Some(new ClassInsertHandler(parsed.getTargetClasses.values.toSet))
      } else None
    } catch {
      case NonFatal(_) => None // If we fail, for any reason, no class browsing
    }
  }

  override def preStart = {
    super.preStart
    context.parent ! RequestPermissionToUpdate
  }

  override def postStop = {
    invalidate
    super.postStop
  }

  def receive = {
    /*
     * Update notification/permission messages
     */
    case PleaseUpdate => initialise
    case DocumentsInserted(docs) => handleInsert(docs)
    case DatasetUpdate(ids) if ids contains dataset.id => initialise
    // Default, for an update message we don't care about
    case _: PleaseUpdate => sender ! AllDone

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
    sender ! DataUpdatedNotification
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
    dataOpt getOrElse {
      val data = dataFactory
      dataOpt = Some(data)
      data
    }
  }

  private def regenerate {
    invalidate
    for (db <- pool) catchingOException(()) {
        updateRecord(db)
        updateClassInsertHandler

        val context = new OBasicCommandContext()
        val browser = browse(db.getUnderlying, context)
        val compiled = new Compiled(context)


        compiled.maybeInsert(browser)
    }
    notifyListeners
  }

  private def handleInsert(docs: Traversable[ODocument]) {
    if (dataOpt == None) initialise
    else {
      classInsertHandler match {
        case None =>
          sender ! RemindMeLater
        case Some(handler) =>
          if (handler.maybeInsert(docs)) notifyListenersLater
          sender ! AllDone
      }
    }
  }

  private def initialise {
    regenerate
    sender ! AllDone
  }

  private def invalidate {
    dataOpt.foreach (_.close)
    dataOpt = None
  }

  private def catchingOException[X](backup: => X)(f: => X): X = {
    try f
    catch {
      case ex: OException => backup
    }
  }

  private def browse(db: ODatabaseDocument, context: OCommandContext): Iterable[ODocument] =
  catchingOException(Iterable.empty[ODocument]){
    val parsed = new OSQLTarget(dataset.table, context, null)

    if (parsed.getTargetClasses() != null) {
      for (clazz <- parsed.getTargetClasses().values().view;
           doc <- (db.browseClass(clazz): Iterable[ODocument])) yield doc
    } else if (parsed.getTargetRecords() != null) {
      parsed.getTargetRecords().view.map (_.getRecord())
    } else if (parsed.getTargetClusters() != null) {
      for (cluster <- parsed.getTargetClusters().values().view;
           doc <- (db.browseCluster[ODocument](cluster): Iterable[ODocument])) yield doc
    } else if (parsed.getTargetIndex() != null) {
      db.getMetadata().getIndexManager().getIndex(parsed.getTargetIndex()).getEntriesBetween(null, null): Iterable[ODocument]
    } else {
      Iterable.empty[ODocument]
    }
  }

  private class ClassInsertHandler(val classes: Set[String]) extends Compiled(null) {
    def handles(tables: Set[String]) = (tables & classes).nonEmpty
    override def maybeInsert(docs: Traversable[ODocument]) = {
      for (db <- pool) yield {
        super.maybeInsert(docs filter {doc => classes contains doc.getClassName.toUpperCase})
      }
    }
  }

  private class Compiled(context: OCommandContext) {
    val filter = compile(dataset.whereClause, context)
    val nameComp = compile(dataset.rowName, context)
    val tsComp = compile(dataset.timestamp, context)
    val metricComp = compile(dataset.metric, context)

    def maybeInsert(docs: Traversable[ODocument]) = {
      (false /: docs){(t, d) => t | maybeInsertDoc(d)}
    }

    def maybeInsertDoc(doc: ODocument): Boolean = catchingOException(false) {
      val filterValue = filter.evaluate(doc, doc, context)
      if (filterValue == java.lang.Boolean.TRUE || filterValue == null) {
        val rowName = nameComp.evaluate(doc, doc, context).toString
        val ts = tsComp.evaluate(doc, doc, context) match {
          case l: java.lang.Long => l.longValue
          case d: java.util.Date => d.getTime()
        }

        val metric = metricComp.evaluate(doc, doc, context) match {
          case n: java.lang.Number => n.doubleValue
        }

        data(rowName) += ts -> metric
        true
      } else false
    }
  }
}