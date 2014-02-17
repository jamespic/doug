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

  val rand = new java.util.Random                 //> rand  : java.util.Random = java.util.Random@4bec88
  val uri = s"memory:${UUID.randomUUID}"          //> uri  : String = memory:471a9b06-1299-4662-847b-328c4b69c88d
  implicit val db = new OObjectDatabaseTx(uri)    //> db  : com.orientechnologies.orient.object.db.OObjectDatabaseTx = OrientDB[me
                                                  //| mory:471a9b06-1299-4662-847b-328c4b69c88d]
  implicit val docDb = db.getUnderlying           //> docDb  : com.orientechnologies.orient.core.db.document.ODatabaseDocument = O
                                                  //| rientDB[memory:471a9b06-1299-4662-847b-328c4b69c88d]
  db.create[OObjectDatabaseTx]                    //> res0: com.orientechnologies.orient.object.db.OObjectDatabaseTx = OrientDB[me
                                                  //| mory:471a9b06-1299-4662-847b-328c4b69c88d] (users: 1)
  uk.me.jamespic.dougng.model.util.initDB

  timeit(for (i <- 1 to 1000000) {
    val sample = Sample("/",new Date(i * 1000L), true, 200, "http://localhost/")
    sample("response_time") = rand.nextInt(5000) + 1000
    sample.save
  })                                              //> Took 31.340721502 seconds

  var dataset = new Dataset                       //> dataset  : <error> = uk.me.jamespic.dougng.model.Dataset@1fddfd4
  dataset.metric = "response_time"                //> res1: <error> = ()
  dataset.rowName = "name"                        //> res2: <error> = ()
  dataset.table = "Sample"                        //> res3: <error> = ()
  dataset.timestamp = "timestamp"                 //> res4: <error> = ()
  dataset = db.save(dataset)
  val dsId = dataset.id                           //> dsId  : <error> = #9:0

  implicit val sys = ActorSystem("MySystem")      //> sys  : akka.actor.ActorSystem = akka://MySystem
  val inbox = ActorDSL.inbox                      //> inbox  : akka.actor.ActorDSL.Inbox = akka.actor.dsl.Inbox$Inbox@1696c4a
  implicit val self = inbox.getRef                //> self  : akka.actor.ActorRef = Actor[akka://MySystem/system/dsl/inbox-1#-518
                                                  //| 444533]

  val dbActor = sys.actorOf(Props(new Database(uri)))
                                                  //> dbActor  : <error> = Actor[akka://MySystem/user/$a#-1773391369]
  dbActor ! GetDataset(dsId)                      //> res5: <error> = ()
  val ActorCreated(dsActor, _) = inbox.receive(5 seconds)
                                                  //> dsActor  : akka.actor.ActorRef = Actor[akka://MySystem/user/$a/dataset-9-0#
                                                  //| 150207680]
  dsActor ! ListenTo
  timeit(inbox.receive(5 minutes) == DataUpdatedNotification)
                                                  //> Took 199.070760541 seconds
                                                  //| res6: Boolean = true
  dsActor ! GetAllSummaries(for (i <- 1L to 100L) yield (i * 10000000L, i * 10000000L + 9909999L), "Response")
  val Summaries(result, _) = timeit(inbox.receive(30 seconds))
                                                  //> Took 24.718393313 seconds
                                                  //| result  : Map[String,Map[(Long, Long),Option[uk.me.jamespic.dougng.util.Det
                                                  //| ailedStats]]] = Map(/ -> Map((350000000,359909999) -> Some(DetailedStats(St
                                                  //| ats(3.4240732E7,9910,5999.0,1000.0,1.3884236394E11),Range(896.0-1024.0) : #
                                                  //| ######
                                                  //| Range(1024.0-1152.0): ####################################
                                                  //| Range(1152.0-1280.0): ######################################
                                                  //| Range(1280.0-1408.0): ##############################
                                                  //| Range(1408.0-1536.0): ######################################
                                                  //| Range(1536.0-1664.0): ########################################
                                                  //| Range(1664.0-1792.0): ###################################
                                                  //| Range(1792.0-1920.0): ####################################
                                                  //| Range(1920.0-2048.0): ###################################
                                                  //| Range(2048.0-2176.0): ##################################
                                                  //| Range(2176.0-2304.0): #####################################
                                                  //| Range(2304.0-2432.0): ######################################
                                                  //| Rang
                                                  //| Output exceeds cutoff limit.
  result("/").size                                //> res7: Int = 100
  dsActor ! GetAllSummaries(for (i <- 1L to 100L) yield (i * 10000000L, i * 10000000L + 9909999L), "Response")
  val Summaries(result2, _) = timeit(inbox.receive(30 seconds))
                                                  //> Took 4.18453827 seconds
                                                  //| result2  : Map[String,Map[(Long, Long),Option[uk.me.jamespic.dougng.util.De
                                                  //| tailedStats]]] = Map(/ -> Map((350000000,359909999) -> Some(DetailedStats(S
                                                  //| tats(3.4240732E7,9910,5999.0,1000.0,1.3884236394E11),Range(896.0-1024.0) : 
                                                  //| #######
                                                  //| Range(1024.0-1152.0): ####################################
                                                  //| Range(1152.0-1280.0): ######################################
                                                  //| Range(1280.0-1408.0): ##############################
                                                  //| Range(1408.0-1536.0): ######################################
                                                  //| Range(1536.0-1664.0): ########################################
                                                  //| Range(1664.0-1792.0): ###################################
                                                  //| Range(1792.0-1920.0): ####################################
                                                  //| Range(1920.0-2048.0): ###################################
                                                  //| Range(2048.0-2176.0): ##################################
                                                  //| Range(2176.0-2304.0): #####################################
                                                  //| Range(2304.0-2432.0): ######################################
                                                  //| Range
                                                  //| Output exceeds cutoff limit.
  result2("/").size                               //> res8: Int = 100
  sys.shutdown
}