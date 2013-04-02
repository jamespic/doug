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
  import com.orientechnologies.orient.core.config.OGlobalConfiguration;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(746); 
  
  //FIXME: Tune the hell out of these
  OGlobalConfiguration.CACHE_LEVEL1_SIZE.setValue(1000);$skip(56); 
  OGlobalConfiguration.CACHE_LEVEL2_SIZE.setValue(5000);$skip(47); 

  val uri = "local:/home/james/orientdb/test";System.out.println("""uri  : String = """ + $show(uri ));$skip(49); 
  implicit val db = new ODatabaseDocumentTx(uri);System.out.println("""db  : com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx = """ + $show(db ));$skip(138); val res$0 = 
  if (!db.exists) {
    db.create():ODatabaseDocumentTx
    ensureSchema
  } else {
    db.open("admin","admin"): ODatabaseDocumentTx
  };System.out.println("""res0: Any = """ + $show(res$0));$skip(1136); 

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
  }}
}
