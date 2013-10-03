package uk.me.jamespic.dougng.model.datamanagement

import akka.actor.Actor
import com.orientechnologies.orient.core.command.{OBasicCommandContext,OCommandContext}
import com.orientechnologies.orient.core.sql.filter.{OSQLTarget, OSQLFilter}
import scala.collection.JavaConversions._
import com.orientechnologies.orient.core.record.impl.ODocument
import scala.collection.mutable.{Map => MMap}
import uk.me.jamespic.dougng.util.MutableMapReduce
import uk.me.jamespic.dougng.util.DetailedStats
import uk.me.jamespic.dougng.model.Dataset
import scala.util.control.NonFatal
import akka.actor.ActorRef
import scala.concurrent.duration._
import resource._
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx

object DatasetActor {
  object ImmutableContext extends OBasicCommandContext {
    variables = java.util.Collections.emptyMap[String, Object]
  }

  private def compile(cmd: String, ctx: OCommandContext) = new OSQLFilter(cmd, ctx, null)

  private def notifyLaterTime = 1 second // FIXME: This should be configurable

  private case object NotificationTime

  def constructor(dataset: Dataset, dataFactory: => DataStore)(pool: ReplacablePool) = {
    new DatasetActor(dataset, dataFactory, pool)
  }
}

class DatasetActor(private var dataset: Dataset, dataFactory: => DataStore, pool: ReplacablePool) extends Actor {
  import DatasetActor._
  private var dataOpt: Option[DataStore] = None
  private var listeners = Set.empty[ActorRef]
  private var notificationPending = false

  private var classInsertHandler: Option[ClassInsertHandler] = None

  private def updateRecord(db: OObjectDatabaseTx) {
    dataset = db.reload(dataset)
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

  override def postRestart(t: Throwable) = {
    super.postRestart(t)
    context.parent ! RequestPermissionToUpdate
  }

  def receive = {
    case update: TablesUpdated => handleUpdate(update)
    case PermissionToUpdate => initialise
    case DocumentInserted(doc) => handleInsert(doc)
    case ListenTo => listenTo
    case UnlistenTo => listeners -= sender
    case GetMetadata => sender ! Metadata(data.min, data.max, data.rows)
    case GetSummaries(rows, ranges) => summaries(rows, ranges)
    case GetAllSummaries(ranges) => summaries(data.rows, ranges)
    case GetInRange(rows, from, to) => getInRange(rows, from, to)
    case GetAllInRange(from, to) => getInRange(data.rows, from, to)
    case NotificationTime => notifyListeners
    case WhichTablesAreYouInterestedIn => whichTables
  }

  private def whichTables {
    classInsertHandler match {
      case None => sender ! AllOfThem
      case Some(handler) => sender ! TheseTables(handler.classes)
    }
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
    sender ! DataUpdatedNotification
  }

  private def summaries(rows: Iterable[String], ranges: Iterable[(Long,Long)]) {
    val result = for (row <- rows) yield {
      val mrData = data(row)
      val rowData = for (r @ (low, high) <- ranges) yield {
        r -> mrData.summaryBetween(low, high)
      }
      row -> rowData.toMap
    }
    sender ! Summaries(result.toMap)
  }

  private def getInRange(rows: Iterable[String], from: Long, to: Long) {
    val result = for (row <- rows) yield row -> data(row).getBetween(from, to).toSeq
    sender ! Ranges(result.toMap)
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
    var updated = false
    for (db <- pool) {
      updateRecord(db)
      updateClassInsertHandler

      val context = new OBasicCommandContext()
      val browser = browse(db.getUnderlying, context)
      val compiled = new Compiled(context)


      for {doc <- browser} {
        if (compiled.maybeInsert(doc)) updated = true
      }
    }
    if (updated) notifyListeners
  }

  private def handleInsert(doc: ODocument) {
    classInsertHandler match {
      case None =>
        sender ! RemindMeLater
      case Some(handler) =>
        if (handler.maybeInsert(doc)) notifyListenersLater
        sender ! AllDone
    }
  }

  private def initialise {
    regenerate
    sender ! AllDone
  }

  private def handleUpdate(update: TablesUpdated) {
    val TablesUpdated(tables) = update
    classInsertHandler match {
      case None => regenerate
      case Some(handler) if handler handles tables => regenerate
      case _ => //  Do nothing
    }
    sender ! AllDone
  }

  private def invalidate {
    dataOpt.foreach (_.close)
    dataOpt = None
  }

  override def postStop = invalidate

  private def browse(db: ODatabaseDocument, context: OCommandContext) = {
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
      List.empty[ODocument]
    }
  }

  private class ClassInsertHandler(val classes: Set[String]) extends Compiled(null) {
    def handles(tables: Set[String]) = (tables & classes).nonEmpty
    override def maybeInsert(doc: ODocument) = {
      if (classes contains doc.getClassName().toUpperCase()) {
        for (db <- pool) yield {
          super.maybeInsert(doc)
        }
      } else false
    }
  }

  private class Compiled(context: OCommandContext) {
    val filter = compile(dataset.whereClause, context)
    val nameComp = compile(dataset.rowName, context)
    val tsComp = compile(dataset.timestamp, context)
    val metricComp = compile(dataset.metric, context)

    def maybeInsert(doc: ODocument): Boolean = {
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