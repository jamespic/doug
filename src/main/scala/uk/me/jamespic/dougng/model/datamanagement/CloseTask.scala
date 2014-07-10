package uk.me.jamespic.dougng.model.datamanagement

import java.util.UUID

import akka.actor.{ActorSystem, Props, ActorRef, ActorDSL}
import com.orientechnologies.orient.`object`.db.OObjectDatabasePool

import scala.concurrent.Promise
import scala.util.Success


object CloseTask {
  case object Closed

  def closeDB(database: ActorRef)(implicit system: ActorSystem) = {
    import ActorDSL._
    val promise = Promise[Unit]()

    val respondTo = actor(new Act {
      become {
        case ActorCreated(closer, name) => // Do nothing
        case CloseTask.Closed =>
          promise.complete(Success(()))
          context.stop(self)
      }
    })

    database.tell(constructMsg(respondTo), respondTo)
    promise.future
  }

  def constructMsg(respondTo: ActorRef) =
    CreateDataDependentActor(pool => Props(classOf[CloseTask], respondTo, pool.database, pool.pool), s"close-task-${UUID.randomUUID}")
}

class CloseTask(respondTo: ActorRef, val database: ActorRef, pool: OObjectDatabasePool) extends ExclusiveTask {
  override def doTask: Unit = {
    context.stop(database)
    pool.close()
    respondTo ! CloseTask.Closed
  }
}
