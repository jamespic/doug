package uk.me.jamespic.dougng.viewmodel

import akka.actor.ActorRef
import scala.collection.JavaConversions._
import uk.me.jamespic.dougng.model.datamanagement._
import uk.me.jamespic.dougng.model.Graph
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import akka.actor.Props

object GraphListViewModel {
  type ViewModelFactory = PartialFunction[Graph, CreateDataDependentActor]
  case class GraphInfo(record: Graph, actorRef: ActorRef)
  def constructMsg(vmFactory: ViewModelFactory) = {
    CreateDataDependentActor(pool => Props(new GraphListViewModel(vmFactory, pool)), "graph-list")
  }
}

//FIXME: Test This
class GraphListViewModel(
    viewModelFactory: GraphListViewModel.ViewModelFactory,
    pool: ReplacablePool) extends SubscribableVariable {
  import GraphListViewModel._

  private var graphActors = Map.empty[String, GraphInfo]
  private var pendingGraphs = Map.empty[String, Graph]

  override def preStart = {
    context.parent ! RequestPermissionToUpdate
  }

  override def receive = super.receive orElse {
    case PleaseUpdate | DocumentsInserted(_) => updateList
    case DatasetUpdate(datasets) => updates(datasets)
    case PresentationUpdate(datasets) => updates(datasets)
    case DocumentsDeleted(docs) => deleteRecords(docs)
    case ActorCreated(actorRef, name) => actorCreated(actorRef, name)
  }

  private def reload(db: OObjectDatabaseTx, graph: Graph) = {
    val reloaded = db.reload[Graph](graph)
    db.detach[Graph](reloaded)
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
    if (updated) maybeFire
    sender ! AllDone
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
    maybeFire
    sender ! AllDone
  }

  private def maybeFire = if (pendingGraphs.isEmpty) fireUpdated(graphActors)

  private def actorCreated(actorRef: ActorRef, name: String) ={
    pendingGraphs.get(name) map {record =>
      pendingGraphs -= name
      graphActors += record.id -> GraphInfo(record, actorRef)
      maybeFire
    }
  }

  private def updateList = {
    for (db <- pool) {
      val graphs: Iterable[Graph] = db.browseClass(classOf[Graph])
      val newGraphs = graphs
          .filter(x => !((graphActors contains x.id)
                           || (pendingGraphs contains x.id)))
      pendingGraphs ++= (for (graph <- newGraphs; if viewModelFactory isDefinedAt graph) yield {
        val request = viewModelFactory(graph)
        context.parent ! request
        request.name -> db.detach[Graph](graph)
      })
    }
    sender ! AllDone
  }
}