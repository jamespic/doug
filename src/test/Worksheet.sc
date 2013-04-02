object Worksheet {
  import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
  import uk.me.jamespic.dougng.model.{Sample => USample, _}
  import util._
  import USample._
  import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
  import scala.collection.JavaConversions._
  import com.orientechnologies.orient.core.query.nativ._
  import com.orientechnologies.orient.`object`.enhancement.OObjectEntityEnhancer
  import scala.reflect.runtime.currentMirror
  import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
  import java.util.Date
  import com.orientechnologies.orient.core.config.OGlobalConfiguration
  
  //FIXME: Tune the hell out of these
  OGlobalConfiguration.CACHE_LEVEL1_SIZE.setValue(1000)
  OGlobalConfiguration.CACHE_LEVEL2_SIZE.setValue(5000)

  val uri = "local:/home/james/orientdb/test"     //> uri  : String = local:/home/james/orientdb/test
  implicit val db = new ODatabaseDocumentTx(uri)  //> db  : com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx = Or
                                                  //| ientDB[local:/home/james/orientdb/test]
  if (!db.exists) {
    db.create():ODatabaseDocumentTx
    ensureSchema
  } else {
    db.open("admin","admin"): ODatabaseDocumentTx
  }                                               //> res0: Any = ()

  try {
    val startTime = System.currentTimeMillis
    def checkIn = println(s"Stopwatch: ${System.currentTimeMillis - startTime}")
    println("Starting creation phase")
    checkIn
    db.declareIntent(new OIntentMassiveInsert)
    var random = new java.util.Random()
    for (i <- 0L to 10000000L) {
      var sample = Sample("Sample", new Date(i * 100))
      sample("responseTime") = random.nextInt(500).toLong
      sample("url") = "http://localhost"
      sample.save()
    }
    println("Complete")
    checkIn
    db.declareIntent(null)
    val ds = new Dataset
    ds.metric = "responseTime"
    ds.whereClause = "name = 'Sample'"
    ds.rowName = "'Row'"
    println("Generating max data")
    checkIn
    val maxData = ds.maxData(db, 30000L)
    checkIn
    println(s"Max data size: ${maxData("Row").size}")
    println(s"First piece of max data: ${maxData("Row")(0L)}")
    println("Generating avg data")
    checkIn
    val avgData = ds.avgData(db, 30000L)
    println(s"Avg data size: ${avgData("Row").size}")
    println(s"First piece of avg data: ${avgData("Row")(0L)}")
    checkIn
  } finally {
    db.close()
  }                                               //> Starting creation phase
                                                  //| Stopwatch: 0
                                                  //| Exception in thread "Thread-0" com.orientechnologies.orient.core.exception.
                                                  //| ODatabaseException: Error on saving record in cluster #1
                                                  //| 	at com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract.e
                                                  //| xecuteSaveRecord(ODatabaseRecordAbstract.java:791)
                                                  //| 	at com.orientechnologies.orient.core.tx.OTransactionNoTx.saveRecord(OTra
                                                  //| nsactionNoTx.java:77)
                                                  //| 	at com.orientechnologies.orient.core.db.record.ODatabaseRecordTx.save(OD
                                                  //| atabaseRecordTx.java:240)
                                                  //| 	at com.orientechnologies.orient.core.db.record.ODatabaseRecordTx.save(OD
                                                  //| atabaseRecordTx.java:36)
                                                  //| 	at com.orientechnologies.orient.core.record.ORecordAbstract.save(ORecord
                                                  //| Abstract.java:294)
                                                  //| 	at com.orientechnologies.orient.core.record.ORecordAbstract.save(ORecord
                                                  //| Abstract.java:285)
                                                  //| 	at com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeEntryDa
                                                  //| taProviderAbstract.save(OMVRBTreeEntryDataProviderAbstract.java:166)
                                                  //| 	at com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeEntryDa
                                                  //| taProviderAbstract.save(OMVRBTreeEntryDataProviderAbstract.java:156)
                                                  //| 	at com.orientechnologies.orient.core.type.tree.OMVRBTreeEntryPersistent.
                                                  //| save(OMVRBTreeEntryPersistent.java:185)
                                                  //| 	at com.orientechnologies.orient.core.type.tree.OMVRBTreePersistent.commi
                                                  //| tChanges(OMVRBTreePersistent.java:517)
                                                  //| 	at com.orientechnologies.orient.core.type.tree.OMVRBTreeDatabaseLazySave
                                                  //| .lazySave(OMVRBTreeDatabaseLazySave.java:70)
                                                  //| 	at com.orientechnologies.orient.core.index.OIndexMVRBTreeAbstract.lazySa
                                                  //| ve(OIndexMVRBTreeAbstract.java:509)
                                                  //| 	at com.orientechnologies.orient.core.index.OIndexMVRBTreeAbstract.flush(
                                                  //| OIndexMVRBTreeAbstract.java:104)
                                                  //| 	at com.orientechnologies.orient.core.index.OIndexManagerAbstract.flush(O
                                                  //| IndexManagerAbstract.java:142)
                                                  //| 	at com.orientechnologies.orient.core.index.OIndexManagerAbstract.close(O
                                                  //| IndexManagerAbstract.java:216)
                                                  //| 	at com.orientechnologies.orient.core.storage.OStorageAbstract.close(OSto
                                                  //| rageAbstract.java:109)
                                                  //| 	at com.orientechnologies.orient.core.storage.OStorageEmbedded.close(OSto
                                                  //| rageEmbedded.java:71)
                                                  //| 	at com.orientechnologies.orient.core.storage.impl.local.OStorageLocal.cl
                                                  //| ose(OStorageLocal.java:346)
                                                  //| 	at com.orientechnologies.orient.core.storage.OStorageAbstract.close(OSto
                                                  //| rageAbstract.java:97)
                                                  //| 	at com.orientechnologies.orient.core.db.raw.ODatabaseRaw.close(ODatabase
                                                  //| Raw.java:508)
                                                  //| 	at com.orientechnologies.orient.core.db.ODatabaseWrapperAbstract.close(O
                                                  //| DatabaseWrapperAbstract.java:67)
                                                  //| 	at com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract.c
                                                  //| lose(ODatabaseRecordAbstract.java:213)
                                                  //| 	at com.orientechnologies.orient.core.db.ODatabaseWrapperAbstract.close(O
                                                  //| DatabaseWrapperAbstract.java:67)
                                                  //| 	at Worksheet$$anonfun$main$1.apply$mcV$sp(Worksheet.scala:61)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$$anonfun$$exe
                                                  //| cute$1.apply$mcV$sp(WorksheetSupport.scala:76)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$.redirected(W
                                                  //| orksheetSupport.scala:65)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$.$execute(Wor
                                                  //| ksheetSupport.scala:75)
                                                  //| 	at Worksheet$.main(Worksheet.scala:13)
                                                  //| 	at Worksheet.main(Worksheet.scala)
                                                  //| Caused by: java.lang.IllegalArgumentException: Cluster segment #1 does not 
                                                  //| exist in database 'test'
                                                  //| 	at com.orientechnologies.orient.core.storage.impl.local.OStorageLocal.ch
                                                  //| eckClusterSegmentIndexRange(OStorageLocal.java:1524)
                                                  //| 	at com.orientechnologies.orient.core.storage.impl.local.OStorageLocal.ge
                                                  //| tClusterById(OStorageLocal.java:1332)
                                                  //| 	at com.orientechnologies.orient.core.db.ODefaultDataSegmentStrategy.assi
                                                  //| gnDataSegmentId(ODefaultDataSegmentStrategy.java:37)
                                                  //| 	at com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract.e
                                                  //| xecuteSaveRecord(ODatabaseRecordAbstract.java:747)
                                                  //| 	... 28 more
}