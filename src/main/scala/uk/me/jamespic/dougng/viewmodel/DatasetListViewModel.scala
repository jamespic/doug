package uk.me.jamespic.dougng.viewmodel

import uk.me.jamespic.dougng.model.datamanagement._
import akka.actor.ActorRef
import uk.me.jamespic.dougng.model.datamanagement.RequestReadOnStart
import uk.me.jamespic.dougng.model.Dataset
import scala.collection.JavaConversions._
import scala.collection.generic.CanBuildFrom
import com.orientechnologies.orient.core.record.impl.ODocument

// FIXME: Test this
class DatasetListViewModel(pool: ReplacablePool, protected val database: ActorRef)
    extends SubscribableVariable with RequestReadOnStart {
  var dsMap = Map.empty[String, Dataset]
  override def receive = super.receive orElse {
    case PleaseRead => initialise
    case DocumentsInserted(docs) => maybeAdd(docs)
    case DocumentsDeleted(docs) => delete(docs)
    case DatasetUpdate(idsAffected) => datasetUpdate(idsAffected)
    case _: PleaseUpdate => sender ! AllDone
  }

  private def delete(docs: Set[String]) = {
    dsMap --= docs
    finished()
  }

  private def maybeAdd(docs: Traversable[ODocument]) = {
    val newDatasets = for (db <- pool;
         doc <- docs;
         if (doc.getClassName equalsIgnoreCase "Dataset")
           && !(dsMap containsKey doc.getIdentity.toString())) yield {
      val attachedDs = db.getUserObjectByRecord(doc, "*:-1", false).asInstanceOf[Dataset]
      attachedDs.id -> db.detachAll[Dataset](attachedDs, true)
    }
    dsMap ++= newDatasets
    finished()
  }

  private def datasetUpdate(idsAffected: Set[String]) = {
    val updated = for (db <- pool;
         (rid, ds) <- dsMap;
         if idsAffected contains rid) yield {
      val reloaded = db.load[Dataset](ds)
      rid -> db.detachAll[Dataset](reloaded, true)
    }
    dsMap ++= updated
    finished()
  }

  private def initialise = {
    for (db <- pool) {
      val dsIterator = db.browseClass(classOf[Dataset]): Iterator[Dataset]
      val dsList = dsIterator.toSeq
      dsMap = dsList.map(ds => ds.id -> ds).toMap
    }
    finished()
  }

  private def finished() = {
    sender ! AllDone
    fireUpdated(dsMap)
  }
}