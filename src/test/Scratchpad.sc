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

  val rand = new java.util.Random                 //> rand  : java.util.Random = java.util.Random@938070
  val uri = s"memory:${UUID.randomUUID}"          //> uri  : String = memory:d17d0b74-f342-4a1a-a49a-ac031556b16f
  implicit val db = new OObjectDatabaseTx(uri)    //> db  : com.orientechnologies.orient.object.db.OObjectDatabaseTx = OrientDB[me
                                                  //| mory:d17d0b74-f342-4a1a-a49a-ac031556b16f]
  implicit val docDb = db.getUnderlying           //> docDb  : com.orientechnologies.orient.core.db.document.ODatabaseDocument = O
                                                  //| rientDB[memory:d17d0b74-f342-4a1a-a49a-ac031556b16f]
  db.create[OObjectDatabaseTx]                    //> res0: com.orientechnologies.orient.object.db.OObjectDatabaseTx = OrientDB[me
                                                  //| mory:d17d0b74-f342-4a1a-a49a-ac031556b16f] (users: 1)
  uk.me.jamespic.dougng.model.util.initDB

  timeit(for (i <- 1 to 1000000) {
    val sample = Sample("/",new Date(i * 1000L), true, 200, "http://localhost/")
    sample("response_time") = rand.nextInt(5000) + 1000
    sample.save
  })                                              //> Took 24.267431591 seconds

  var dataset = new Dataset                       //> dataset  : uk.me.jamespic.dougng.model.Dataset = uk.me.jamespic.dougng.mode
                                                  //| l.Dataset@10ff8a4
  dataset.metric = "response_time"
  dataset.rowName = "name"
  dataset.table = "Sample"
  dataset.timestamp = "timestamp"
  dataset = db.save(dataset)
  val dsId = dataset.id                           //> dsId  : String = #9:0

  implicit val sys = ActorSystem("MySystem")      //> sys  : akka.actor.ActorSystem = akka://MySystem
  val inbox = ActorDSL.inbox                      //> inbox  : akka.actor.ActorDSL.Inbox = akka.actor.dsl.Inbox$Inbox@1a483c8
  implicit val self = inbox.getRef                //> self  : akka.actor.ActorRef = Actor[akka://MySystem/system/dsl/inbox-1#-653
                                                  //| 280539]

  val dbActor = sys.actorOf(Props(new Database(uri)))
                                                  //> dbActor  : akka.actor.ActorRef = Actor[akka://MySystem/user/$a#1291192219]
  dbActor ! GetDataset(dsId)
  val ActorCreated(dsActor, _) = inbox.receive(5 seconds)
                                                  //> dsActor  : akka.actor.ActorRef = Actor[akka://MySystem/user/$a/dataset-9-0#
                                                  //| 384984870]
  dsActor ! ListenTo
  timeit(inbox.receive(5 minutes) == DataUpdatedNotification)
                                                  //> Took 100.992987657 seconds
                                                  //| res1: Boolean = true
  dsActor ! GetAllSummaries(for (i <- 1L to 100L) yield (i * 10000000L, i * 10000000L + 9909999L), "Response")
  val Summaries(result, _) = timeit(inbox.receive(90 seconds))
                                                  //> Took 91.209823608 seconds
                                                  //| java.util.concurrent.TimeoutException: deadline passed
                                                  //| 	at akka.actor.dsl.Inbox$InboxActor$$anonfun$receive$1.applyOrElse(Inbox.
                                                  //| scala:117)
                                                  //| 	at scala.PartialFunction$AndThen.applyOrElse(PartialFunction.scala:184)
                                                  //| 	at akka.actor.Actor$class.aroundReceive(Actor.scala:465)
                                                  //| 	at akka.actor.dsl.Inbox$InboxActor.aroundReceive(Inbox.scala:62)
                                                  //| 	at akka.actor.ActorCell.receiveMessage(ActorCell.scala:491)
                                                  //| 	at akka.actor.ActorCell.invoke(ActorCell.scala:462)
                                                  //| 	at akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)
                                                  //| 	at akka.dispatch.Mailbox.run(Mailbox.scala:220)
                                                  //| 	at akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(Abst
                                                  //| ractDispatcher.scala:393)
                                                  //| 	at scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)
                                                  //| 	at scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool
                                                  //| .java:1339)
                                                  //| 	at scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java
                                                  //| Output exceeds cutoff limit.\
  result("/").size
  dsActor ! GetAllSummaries(for (i <- 1L to 100L) yield (i * 10000000L, i * 10000000L + 9909999L), "Response")
  val Summaries(result2, _) = timeit(inbox.receive(30 seconds))
  result2("/").size
  Thread.sleep(60000L)
  sys.shutdown
}