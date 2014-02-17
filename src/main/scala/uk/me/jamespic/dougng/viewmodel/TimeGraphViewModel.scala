package uk.me.jamespic.dougng.viewmodel

import akka.actor.ActorRef
import uk.me.jamespic.dougng.model.datamanagement._
import com.orientechnologies.orient.core.id.ORecordId
import uk.me.jamespic.dougng.model.TimeGraph
import scala.collection.JavaConversions._
import uk.me.jamespic.dougng.model.DatasetName
import scala.collection.SortedMap
import uk.me.jamespic.dougng.util.DetailedStats
import akka.actor.Props
import uk.me.jamespic.dougng.model.TimeGraphViewModelName

object TimeGraphViewModel {
  def constructMsg(recordId: String) = {
    CreateDataDependentActor(consInfo => Props(new TimeGraphViewModel(recordId, consInfo.pool, consInfo.database)), TimeGraphViewModelName(recordId))
  }
  val viewModelFactory: GraphListViewModel.ViewModelFactory = {
    case x: TimeGraph => constructMsg(x.id)
  }
}

class TimeGraphViewModel(recordId: String, pool: ReplacablePool, database: ActorRef) extends SubscribableVariable {
  type Row = SortedMap[(Long, Long), Double]
  type Table = SortedMap[String, Row]
  import context.dispatcher
  private var record: Option[TimeGraph] = None
  private var receivedDatasets = Map.empty[String, DatasetInfo]
  private var graphData: Table = SortedMap.empty
  private var version = 0L

  override def receive = super.receive orElse {
    case x: PleaseUpdate => handleDataUpdate(x)
    case ActorCreated(actor, DatasetName(rid)) => handleReceivedDataset(rid, actor)
    case data: Metadata => handleMetadata(data)
    case data: Summaries => handleData(data)
    case DataUpdatedNotification => dataUpdated
  }

  private def handleDataUpdate(msg: PleaseUpdate) = msg match {
    case PleaseRead => initialise
    case DatasetUpdate(ids) if ((ids & (receivedDatasets.keySet + recordId)).nonEmpty) => initialise
    case DocumentsDeleted(docs) if docs contains recordId => shutdown
    case _ => database ! AllDone
  }

  private def handleReceivedDataset(rid: String, actor: ActorRef) = {
    if (receivedDatasets contains rid) {
      receivedDatasets(rid).actor = Some(actor)
      actor ! GetMetadata(version)
      actor ! ListenTo
    }
  }

  private def shutdown = {
    context.stop(self)
  }

  private def initialise = {
    clearOldData
    for (db <- pool) {
      var dbo = db.load[TimeGraph](new ORecordId(recordId))
      if (dbo == null) {
    	shutdown
      } else {
        var detached = db.detachAll[TimeGraph](dbo, true)
        record = Some(detached)
        receivedDatasets = (for (ds <- detached.datasets ++ detached.maxDatasets) yield {
          database ! GetDataset(ds.id)
          ds.id -> new DatasetInfo()
        }).toMap
        database ! AllDone
      }
    }
  }

  def clearOldData = {
    for (dsInfo <- receivedDatasets.values; actor <- dsInfo.actor) actor ! UnlistenTo
    version += 1L
  }

  private def handleMetadata(data: Metadata) = {
    val DatasetName(rid) = sender.path.name
    if ((receivedDatasets contains rid) && (data.corrId == version)) {
      receivedDatasets(rid).metadata = Some(data)
    }
    if (receivedDatasets.values.forall(_.metadata != None)) startBuildingDataset
  }

  private def handleData(data: Summaries) = {
    val Summaries(result, _) = data
    val DatasetName(rid) = sender.path.name
    if ((receivedDatasets contains rid) && (data.corrId == version)) {
      receivedDatasets(rid).data = Some(result)
    }
    if (receivedDatasets.values.forall(_.data != None)) rebuildGraphData
  }

  private def rebuildGraphData = {
    graphData = SortedMap.empty
    for ((rid, dsInfo) <- receivedDatasets; data <- dsInfo.data; (row, rowData) <- data) {
      val isMaxDataset = record.get.maxDatasets contains rid
      val tidiedRowData = for ((range, stats) <- rowData; s <- stats) yield {
        (range, if (isMaxDataset) s.getMax else s.getMean)
      }
      // In the event of duplicate rows, some of them will be suffixed by dataset id
      val rowLabel = if (graphData contains row) s"$row ($rid)" else row
      graphData += rowLabel -> (SortedMap.empty[(Long, Long), Nothing] ++ tidiedRowData)
    }
    fireUpdated(graphData)
  }

  private def dataUpdated = {
    sender ! GetMetadata(version)
  }

  private def startBuildingDataset = {
    val metaData = for (dsInfo <- receivedDatasets.values;
                        meta <- dsInfo.metadata) yield meta
    val minTime = metaData.map(_.min).min
    val maxTime = metaData.map(_.max).max
    val granularity = record.get.granularity
    val ranges = for (start <- minTime to maxTime by granularity) yield (start, start + granularity - 1)
    for (dsInfo <- receivedDatasets.values) {
      val actor = dsInfo.actor.get
      val rows = dsInfo.metadata.get.rows
      actor ! GetSummaries(rows, ranges, version)
    }
  }

  override def preStart = {
    super.preStart
    database ! RequestPermissionToRead
  }

  private class DatasetInfo(var actor: Option[ActorRef] = None,
      var metadata: Option[Metadata] = None,
      var data: Option[Map[String, Map[(Long, Long), Option[DetailedStats]]]] = None)
}