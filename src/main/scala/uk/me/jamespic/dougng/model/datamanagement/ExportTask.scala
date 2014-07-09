package uk.me.jamespic.dougng.model.datamanagement

import java.io.OutputStream
import java.util.UUID

import akka.actor._
import com.orientechnologies.orient.`object`.db.OObjectDatabasePool
import com.orientechnologies.orient.core.command.OCommandOutputListener
import com.orientechnologies.orient.core.db.tool.ODatabaseExport
import play.api.libs.iteratee.{Enumerator, Concurrent, Input}
import uk.me.jamespic.dougng.model.util._

object ExportTask {
  case class DataFragment(input: Input[Array[Byte]])

  def export(database: ActorRef)(implicit context: ActorRefFactory): Enumerator[Array[Byte]] = {
    var exportHandlingActor = ActorRef.noSender
    Concurrent.unicast[Array[Byte]] (onStart = { channel =>
      exportHandlingActor = context.actorOf(Props(new Actor {

        override def preStart() = {
          super.preStart()
          database ! constructMsg(self)
        }

        def receive = init

        def init: Receive = {
          case ActorCreated(exporter: ActorRef, _: String) => context become receiveData
        }

        def receiveData: Receive = {
          case DataFragment(input) => channel.push(input)
        }
      }))
    },
    onComplete = exportHandlingActor ! PoisonPill,
    onError = (_,_) => exportHandlingActor ! PoisonPill)(context.dispatcher)
  }

  def constructMsg(receiver: ActorRef) =
    CreateDataDependentActor(pool => Props(new ExportTask(receiver, pool.database, pool.pool)), s"exporter-${UUID.randomUUID}")
}

class ExportTask(receiver: ActorRef, val database: ActorRef, pool: OObjectDatabasePool) extends ExclusiveTask {
  import ExportTask._
  override def doTask: Unit = {
    for (db <- pool) {
      db.freeze()
      try {
        new ODatabaseExport(db.getUnderlying.getUnderlying, new OutputStream {
          override def write(b: Int): Unit = write(Array(b.toByte))
          override def write(b: Array[Byte]): Unit = receiver ! DataFragment(Input.El(b))
          override def write(b: Array[Byte], off: Int, len: Int): Unit = write(b.slice(off, off + len))
          override def close(): Unit = {
            receiver ! DataFragment(Input.EOF)
            context.stop(self)
            super.close()
          }
        }, new OCommandOutputListener {override def onMessage(iText: String): Unit = ()}).exportDatabase()
      } finally {
        db.release()
      }
    }
  }
}
