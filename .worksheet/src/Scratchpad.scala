//package uk.me.jamespic.dougng

object Scratchpad {
  import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
  import java.util.{UUID, Date}
  import uk.me.jamespic.dougng.model.util._
  import uk.me.jamespic.dougng.model.Sample._
  import scala.concurrent.duration._
  import akka.actor._
  import uk.me.jamespic.dougng.model.datamanagement._
  import uk.me.jamespic.dougng.model.Dataset;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(435); 

  val rand = new java.util.Random;System.out.println("""rand  : java.util.Random = """ + $show(rand ));$skip(41); 
  val uri = s"memory:${UUID.randomUUID}";System.out.println("""uri  : String = """ + $show(uri ));$skip(47); 
  implicit val db = new OObjectDatabaseTx(uri);System.out.println("""db  : com.orientechnologies.orient.object.db.OObjectDatabaseTx = """ + $show(db ));$skip(40); 
  implicit val docDb = db.getUnderlying;System.out.println("""docDb  : com.orientechnologies.orient.core.db.document.ODatabaseDocument = """ + $show(docDb ));$skip(31); val res$0 = 
  db.create[OObjectDatabaseTx];System.out.println("""res0: com.orientechnologies.orient.object.db.OObjectDatabaseTx = """ + $show(res$0));$skip(42); 
  uk.me.jamespic.dougng.model.util.initDB;$skip(183); 

  for (i <- 1 to 1000) {
    val sample = Sample("/",new Date(i * 1000L), true, 200, "http://localhost/")
    sample("response_time") = rand.nextInt(5000) + 1000
    sample.save
  };$skip(29); 

  var dataset = new Dataset;System.out.println("""dataset  : <error> = """ + $show(dataset ));$skip(35); val res$1 = 
  dataset.metric = "response_time";System.out.println("""res1: <error> = """ + $show(res$1));$skip(27); val res$2 = 
  dataset.rowName = "name";System.out.println("""res2: <error> = """ + $show(res$2));$skip(27); val res$3 = 
  dataset.table = "Sample";System.out.println("""res3: <error> = """ + $show(res$3));$skip(34); val res$4 = 
  dataset.timestamp = "timestamp";System.out.println("""res4: <error> = """ + $show(res$4));$skip(29); 
  dataset = db.save(dataset);$skip(24); 
  val dsId = dataset.id;System.out.println("""dsId  : <error> = """ + $show(dsId ));$skip(46); 

  implicit val sys = ActorSystem("MySystem");System.out.println("""sys  : akka.actor.ActorSystem = """ + $show(sys ));$skip(29); 
  val inbox = ActorDSL.inbox;System.out.println("""inbox  : akka.actor.ActorDSL.Inbox = """ + $show(inbox ));$skip(35); 
  implicit val self = inbox.getRef;System.out.println("""self  : akka.actor.ActorRef = """ + $show(self ));$skip(55); 

  val dbActor = sys.actorOf(Props(new Database(uri)));System.out.println("""dbActor  : <error> = """ + $show(dbActor ));$skip(29); val res$5 = 
  dbActor ! GetDataset(dsId);System.out.println("""res5: <error> = """ + $show(res$5));$skip(58); 
  val ActorCreated(dsActor, _) = inbox.receive(5 seconds);System.out.println("""dsActor  : akka.actor.ActorRef = """ + $show(dsActor ));$skip(102); 
  dsActor ! GetAllSummaries(for (i <- 1L to 100L) yield (i * 10000L, i * 10000L + 9999L), "Response");$skip(60); 
  val Summaries(result, corrId) = inbox.receive(10 seconds);System.out.println("""result  : Map[String,Map[(Long, Long),Option[uk.me.jamespic.dougng.util.DetailedStats]]] = """ + $show(result ));System.out.println("""corrId  : Any = """ + $show(corrId ));$skip(15); 
  sys.shutdown}
}
