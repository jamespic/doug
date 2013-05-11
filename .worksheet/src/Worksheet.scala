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
  import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(903); 

  //FIXME: Tune the hell out of these
  OGlobalConfiguration.CACHE_LEVEL1_SIZE.setValue(1000);$skip(56); 
  OGlobalConfiguration.CACHE_LEVEL2_SIZE.setValue(5000);$skip(47); 

  val uri = "local:/home/james/orientdb/test";System.out.println("""uri  : String = """ + $show(uri ));$skip(49); 
  implicit val db = new ODatabaseDocumentTx(uri);System.out.println("""db  : com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx = """ + $show(db ));$skip(347); val res$0 = 
  if (!db.exists) {
    db.create():ODatabaseDocumentTx
    val schema = db.getMetadata().getSchema()
    val sampleCls = schema.createClass("Sample")
    val prop = sampleCls.createProperty("timestamp", OType.DATETIME)
    prop.createIndex(INDEX_TYPE.NOTUNIQUE)
    //ensureSchema
  } else {
    db.open("admin","admin"): ODatabaseDocumentTx
  };System.out.println("""res0: Object = """ + $show(res$0));$skip(1436); 

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
  }}
}
