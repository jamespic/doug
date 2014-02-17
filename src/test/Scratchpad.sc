package uk.me.jamespic.dougng

object Scratchpad {
  import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
  import java.util.{UUID, Date}
  import uk.me.jamespic.dougng.model.util._
  import uk.me.jamespic.dougng.model.Sample._
  import scala.concurrent.duration._
  import akka.actor._
  import uk.me.jamespic.dougng.model.datamanagement._
  import uk.me.jamespic.dougng.model.Dataset

  def timeit[A](f: => A) = {
    val startTime = System.nanoTime
    try f
    finally println(s"Took ${(System.nanoTime - startTime) / 1000000000.0} seconds")
  }                                               //> timeit: [A](f: => A)A

  val rand = new java.util.Random                 //> rand  : java.util.Random = java.util.Random@15b2e86
  val uri = s"memory:${UUID.randomUUID}"          //> uri  : String = memory:39acd71a-6762-4596-bef4-98eb8b36cbce
  implicit val db = new OObjectDatabaseTx(uri)    //> db  : com.orientechnologies.orient.object.db.OObjectDatabaseTx = OrientDB[me
                                                  //| mory:39acd71a-6762-4596-bef4-98eb8b36cbce]
  implicit val docDb = db.getUnderlying           //> docDb  : com.orientechnologies.orient.core.db.document.ODatabaseDocument = O
                                                  //| rientDB[memory:39acd71a-6762-4596-bef4-98eb8b36cbce]
  db.create[OObjectDatabaseTx]                    //> res0: com.orientechnologies.orient.object.db.OObjectDatabaseTx = OrientDB[me
                                                  //| mory:39acd71a-6762-4596-bef4-98eb8b36cbce] (users: 1)
  uk.me.jamespic.dougng.model.util.initDB

  timeit(for (i <- 1 to 1000000) {
    val sample = Sample("/",new Date(i * 1000L), true, 200, "http://localhost/")
    sample("response_time") = rand.nextInt(5000) + 1000
    sample.save
  })                                              //> Took 24.093767884 seconds

  var dataset = new Dataset                       //> dataset  : uk.me.jamespic.dougng.model.Dataset = uk.me.jamespic.dougng.mode
                                                  //| l.Dataset@1f820f3
  dataset.metric = "response_time"
  dataset.rowName = "name"
  dataset.table = "Sample"
  dataset.timestamp = "timestamp"
  dataset = db.save(dataset)
  val dsId = dataset.id                           //> dsId  : String = #9:0

  implicit val sys = ActorSystem("MySystem")      //> sys  : akka.actor.ActorSystem = akka://MySystem
  val inbox = ActorDSL.inbox                      //> inbox  : akka.actor.ActorDSL.Inbox = akka.actor.dsl.Inbox$Inbox@10b1cd3
  implicit val self = inbox.getRef                //> self  : akka.actor.ActorRef = Actor[akka://MySystem/system/dsl/inbox-1#1180
                                                  //| 81565]

  val dbActor = sys.actorOf(Props(new Database(uri)))
                                                  //> dbActor  : akka.actor.ActorRef = Actor[akka://MySystem/user/$a#-631167525]
  dbActor ! GetDataset(dsId)
  val ActorCreated(dsActor, _) = inbox.receive(5 seconds)
                                                  //> dsActor  : akka.actor.ActorRef = Actor[akka://MySystem/user/$a/dataset-9-0#
                                                  //| 1924918680]
  dsActor ! ListenTo
  timeit(inbox.receive(5 minutes) == DataUpdatedNotification)
                                                  //> Took 100.507109742 seconds
                                                  //| res1: Boolean = true
  dsActor ! GetAllSummaries(for (i <- 1L to 100L) yield (i * 10000000L, i * 10000000L + 9909999L), "Response")
  val Summaries(result, _) = timeit(inbox.receive(30 seconds))
                                                  //> Took 13.142187276 seconds
                                                  //| result  : Map[String,Map[(Long, Long),Option[uk.me.jamespic.dougng.util.Det
                                                  //| ailedStats]]] = Map(/ -> Map((350000000,359909999) -> Some(DetailedStats(St
                                                  //| ats(3.43128E7,9910,5998.0,1000.0,1.39055252824E11),Range(896.0-1024.0) : ##
                                                  //| ####
                                                  //| Range(1024.0-1152.0): ###################################
                                                  //| Range(1152.0-1280.0): #######################################
                                                  //| Range(1280.0-1408.0): ###################################
                                                  //| Range(1408.0-1536.0): ####################################
                                                  //| Range(1536.0-1664.0): ################################
                                                  //| Range(1664.0-1792.0): ####################################
                                                  //| Range(1792.0-1920.0): ##################################
                                                  //| Range(1920.0-2048.0): ####################################
                                                  //| Range(2048.0-2176.0): ####################################
                                                  //| Range(2176.0-2304.0): ###################################
                                                  //| Range(2304.0-2432.0): ##################################
                                                  //| Range(2432.0-25
                                                  //| Output exceeds cutoff limit.
  result("/").size                                //> res2: Int = 100
  dsActor ! GetAllSummaries(for (i <- 1L to 100L) yield (i * 10000000L, i * 10000000L + 9909999L), "Response")
  val Summaries(result2, _) = timeit(inbox.receive(30 seconds))
                                                  //> Took 2.710732222 seconds
                                                  //| result2  : Map[String,Map[(Long, Long),Option[uk.me.jamespic.dougng.util.De
                                                  //| tailedStats]]] = Map(/ -> Map((350000000,359909999) -> Some(DetailedStats(S
                                                  //| tats(3.43128E7,9910,5998.0,1000.0,1.39055252824E11),Range(896.0-1024.0) : #
                                                  //| #####
                                                  //| Range(1024.0-1152.0): ###################################
                                                  //| Range(1152.0-1280.0): #######################################
                                                  //| Range(1280.0-1408.0): ###################################
                                                  //| Range(1408.0-1536.0): ####################################
                                                  //| Range(1536.0-1664.0): ################################
                                                  //| Range(1664.0-1792.0): ####################################
                                                  //| Range(1792.0-1920.0): ##################################
                                                  //| Range(1920.0-2048.0): ####################################
                                                  //| Range(2048.0-2176.0): ####################################
                                                  //| Range(2176.0-2304.0): ###################################
                                                  //| Range(2304.0-2432.0): ##################################
                                                  //| Range(2432.0-25
                                                  //| Output exceeds cutoff limit.
  result2("/").size                               //> res3: Int = 100|
  Thread.sleep(60000L) 
  sys.shutdown
}