package uk.me.jamespic.dougng.viewmodel

import akka.actor.ActorRef
import scala.collection.JavaConversions._
import uk.me.jamespic.dougng.model.datamanagement._
import uk.me.jamespic.dougng.model.Graph
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import akka.actor.Props
import uk.me.jamespic.dougng.model.datamanagement.RequestReadOnStart
import akka.actor.Terminated
import com.orientechnologies.orient.core.record.impl.ODocument

object GraphListViewModel {
  type ViewModelFactory = PartialFunction[Graph, CreateDataDependentActor]
  case class GraphInfo(record: Graph, actorRef: ActorRef)
  def constructMsg(vmFactory: ViewModelFactory) = {
    CreateDataDependentActor(consInfo => Props(new GraphListViewModel(vmFactory, consInfo.pool, consInfo.database)), "graph-list")
  }
}

class GraphListViewModel(
    viewModelFactory: GraphListViewModel.ViewModelFactory,
    pool: ReplacablePool,
    protected val database: ActorRef) extends SubscribableVariable with RequestReadOnStart {
  import GraphListViewModel._

  private var graphActors = Map.empty[String, GraphInfo]
  private var pendingGraphs = Map.empty[String, Graph]

  override def receive = super.receive orElse {
    case PleaseRead => updateList
    case DocumentsInserted(docs) => docsInserted(docs)
    case DatasetUpdate(datasets) => updates(datasets)
    case PresentationUpdate(datasets) => updates(datasets)
    case DocumentsDeleted(docs) => deleteRecords(docs)
    case ActorCreated(actorRef, name) => actorCreated(actorRef, name)
    case Terminated(actor) => handleTermination(actor)
  }

  private def reload(db: OObjectDatabaseTx, graph: Graph) = {
    val reloaded = db.reload[Graph](graph)
    db.detachAll[Graph](reloaded, true)
  }

  private def deleteRecords(docs: Set[String]) = {
    val (victims, survivors) = graphActors partition {docs contains _._1}
    graphActors = survivors
    var updated = false
    for ((_, GraphInfo(record, actorRef)) <- victims) {
      updated = true
      context.stop(actorRef)
    }
    for ((name, record) <- pendingGraphs; if docs contains record.id) {
      pendingGraphs -= name
    }
    database ! AllDone
    if (updated) maybeFire
  }

  private def updates(datasets: Set[String]) = {
    var changed = false
    for (db <- pool) {
      for ((recordId, GraphInfo(graph, actorRef)) <- graphActors; if datasets contains recordId) {
        changed = true
        graphActors += recordId -> GraphInfo(reload(db, graph), actorRef)
      }

      for ((name, graph) <- pendingGraphs; if datasets contains graph.id) {
        pendingGraphs += name -> reload(db, graph)
      }
    }
    database ! AllDone
    maybeFire
  }

  private def maybeFire = if (pendingGraphs.isEmpty) fireUpdated(graphActors)

  private def actorCreated(actorRef: ActorRef, name: String) ={
    pendingGraphs.get(name) map {record =>
      pendingGraphs -= name
      graphActors += record.id -> GraphInfo(record, actorRef)
      context.watch(actorRef)
      maybeFire
    }
  }

  private def handleTermination(actor: ActorRef) = {
    /*
     * If we detect premature termination of an actor, we remove it
     * from our list and request permission to update.
     */
    val affected = graphActors.filter(actor ==  _._2.actorRef)
    if (affected.nonEmpty) {
      graphActors --= affected.keys
      database ! RequestPermissionToRead
    }
  }

  private def docsInserted(docs: Traversable[ODocument]) = {
    for (db <- pool) {
      val graphs =
        for (doc <- docs;
             if doc.getSchemaClass.isSubClassOf("Graph")) yield {
          val graph = db.getUserObjectByRecord(doc, "*:-1", false).asInstanceOf[Graph]
          db.detachAll[Graph](graph, true)
        }
      createNewGraphs(db, graphs)
    }
    database ! AllDone
  }

  private def createNewGraphs(db: OObjectDatabaseTx, graphs: Traversable[Graph]) = {
    val newGraphs = graphs
          .filter(x => !((graphActors contains x.id)
                           || (pendingGraphs contains x.id)))
      pendingGraphs ++= (for (graph <- newGraphs; if viewModelFactory isDefinedAt graph) yield {
        val request = viewModelFactory(graph)
        database ! request
        request.name -> db.detachAll[Graph](graph, true)
      })
  }

  private def updateList = {
    for (db <- pool) {
      val graphs = (db.browseClass(classOf[Graph]): Iterable[Graph]).toSet
      // Remove any graphs that no longer exist
      graphActors = graphActors filter {case (rid, _) => graphs.map(_.id) contains rid}
      // Request ViewModels for any new graphs
      createNewGraphs(db, graphs)
    }
    database ! AllDone
    maybeFire
  }
}