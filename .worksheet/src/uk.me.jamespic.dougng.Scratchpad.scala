package uk.me.jamespic.dougng

object Scratchpad {
  import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
  import java.util.{UUID, Date}
  import uk.me.jamespic.dougng.model.util._
  import uk.me.jamespic.dougng.model.Sample._
  import scala.concurrent.duration._
  import akka.actor._
  import uk.me.jamespic.dougng.model.datamanagement._
  import uk.me.jamespic.dougng.model.Dataset;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(563); 

  def timeit[A](f: => A) = {
    val startTime = System.nanoTime
    try f
    finally println(s"Took ${(System.nanoTime - startTime) / 1000000000.0} seconds")
  };System.out.println("""timeit: [A](f: => A)A""");$skip(35); 

  val rand = new java.util.Random;System.out.println("""rand  : java.util.Random = """ + $show(rand ));$skip(41); 
  val uri = s"memory:${UUID.randomUUID}";System.out.println("""uri  : String = """ + $show(uri ));$skip(47); 
  implicit val db = new OObjectDatabaseTx(uri);System.out.println("""db  : com.orientechnologies.orient.object.db.OObjectDatabaseTx = """ + $show(db ));$skip(40); 
  implicit val docDb = db.getUnderlying;System.out.println("""docDb  : com.orientechnologies.orient.core.db.document.ODatabaseDocument = """ + $show(docDb ));$skip(31); val res$0 = 
  db.create[OObjectDatabaseTx];System.out.println("""res0: com.orientechnologies.orient.object.db.OObjectDatabaseTx = """ + $show(res$0));$skip(42); 
  uk.me.jamespic.dougng.model.util.initDB;$skip(194); 

  timeit(for (i <- 1 to 1000000) {
    val sample = Sample("/",new Date(i * 1000L), true, 200, "http://localhost/")
    sample("response_time") = rand.nextInt(5000) + 1000
    sample.save
  });$skip(29); 

  var dataset = new Dataset;System.out.println("""dataset  : uk.me.jamespic.dougng.model.Dataset = """ + $show(dataset ));$skip(35); 
  dataset.metric = "response_time";$skip(27); 
  dataset.rowName = "name";$skip(27); 
  dataset.table = "Sample";$skip(34); 
  dataset.timestamp = "timestamp";$skip(29); 
  dataset = db.save(dataset);$skip(24); 
  val dsId = dataset.id;System.out.println("""dsId  : String = """ + $show(dsId ));$skip(46); 

  implicit val sys = ActorSystem("MySystem");System.out.println("""sys  : akka.actor.ActorSystem = """ + $show(sys ));$skip(29); 
  val inbox = ActorDSL.inbox;System.out.println("""inbox  : akka.actor.ActorDSL.Inbox = """ + $show(inbox ));$skip(35); 
  implicit val self = inbox.getRef;System.out.println("""self  : akka.actor.ActorRef = """ + $show(self ));$skip(55); 

  val dbActor = sys.actorOf(Props(new Database(uri)));System.out.println("""dbActor  : akka.actor.ActorRef = """ + $show(dbActor ));$skip(29); 
  dbActor ! GetDataset(dsId);$skip(58); 
  val ActorCreated(dsActor, _) = inbox.receive(5 seconds);System.out.println("""dsActor  : akka.actor.ActorRef = """ + $show(dsActor ));$skip(21); 
  dsActor ! ListenTo;$skip(62); val res$1 = 
  timeit(inbox.receive(5 minutes) == DataUpdatedNotification);System.out.println("""res1: Boolean = """ + $show(res$1));$skip(111); 
  dsActor ! GetAllSummaries(for (i <- 1L to 100L) yield (i * 10000000L, i * 10000000L + 9909999L), "Response");$skip(63); 
  val Summaries(result, _) = timeit(inbox.receive(90 seconds));System.out.println("""result  : Map[String,Map[(Long, Long),Option[uk.me.jamespic.dougng.util.DetailedStats]]] = """ + $show(result ));$skip(19); val res$2 = 
  result("/").size;System.out.println("""res2: Int = """ + $show(res$2));$skip(111); 
  dsActor ! GetAllSummaries(for (i <- 1L to 100L) yield (i * 10000000L, i * 10000000L + 9909999L), "Response");$skip(64); 
  val Summaries(result2, _) = timeit(inbox.receive(30 seconds));System.out.println("""result2  : Map[String,Map[(Long, Long),Option[uk.me.jamespic.dougng.util.DetailedStats]]] = """ + $show(result2 ));$skip(20); val res$3 = 
  result2("/").size;System.out.println("""res3: Int = """ + $show(res$3));$skip(23); 
  Thread.sleep(60000L);$skip(15); 
  sys.shutdown}
}
