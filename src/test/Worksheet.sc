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
  import java.util.{Date, List => JList}
  import com.orientechnologies.orient.core.config.OGlobalConfiguration
  import com.orientechnologies.orient.core.metadata.schema.OType
  import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE

  //FIXME: Tune the hell out of these
  OGlobalConfiguration.CACHE_LEVEL1_SIZE.setValue(1000)
  OGlobalConfiguration.CACHE_LEVEL2_SIZE.setValue(5000)

  val uri = "local:/home/james/orientdb/test"     //> uri  : String = local:/home/james/orientdb/test
  implicit val db = new ODatabaseDocumentTx(uri)  //> db  : com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx = O
                                                  //| rientDB[local:/home/james/orientdb/test]
  if (!db.exists) {
    db.create():ODatabaseDocumentTx
    val schema = db.getMetadata().getSchema()
    val sampleCls = schema.createClass("Sample")
    val prop = sampleCls.createProperty("timestamp", OType.DATETIME)
    prop.createIndex(INDEX_TYPE.NOTUNIQUE)
    //ensureSchema
  } else {
    db.open("admin","admin"): ODatabaseDocumentTx
  }                                               //> res0: Object = Sample.timestamp

  try {
    val startTime = System.currentTimeMillis
    def checkIn = println(s"Stopwatch: ${System.currentTimeMillis - startTime}")
    println("Starting creation phase")
    checkIn
    db.declareIntent(new OIntentMassiveInsert)
    var random = new java.util.Random()
    for (i <- 0L to 1000L) {
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
    println("Querying index")
    println
    println((db.query(new OSQLSynchQuery("select key, rid.timestamp, rid.url from index:Sample.timestamp")):JList[_]).get(0))
    checkIn
    println("Querying table")
    println((db.query(new OSQLSynchQuery("select from Sample")):JList[_]).get(0))
    checkIn
  } finally {
    db.close()
  }                                               //> Starting creation phase
                                                  //| Stopwatch: 2
                                                  //| Complete
                                                  //| Stopwatch: 750
                                                  //| Generating max data
                                                  //| Stopwatch: 751
                                                  //| Stopwatch: 1268
                                                  //| Max data size: 4
                                                  //| First piece of max data: 496.0
                                                  //| Generating avg data
                                                  //| Stopwatch: 1269
                                                  //| Avg data size: 4
                                                  //| First piece of avg data: 251.21333333333334
                                                  //| Stopwatch: 1490
                                                  //| Querying index
                                                  //| 
                                                  //| #-2:1{key:Thu Jan 01 01:00:00 GMT 1970,rid:Thu Jan 01 01:00:00 GMT 1970,rid
                                                  //| 2:http://localhost} v0
                                                  //| Stopwatch: 1713
                                                  //| Querying table
                                                  //| Sample#8:0{name:Sample,timestamp:Thu Jan 01 01:00:00 GMT 1970,responseTime:
                                                  //| 226,url:http://localhost} v0
                                                  //| Stopwatch: 1799
}