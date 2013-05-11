import com.orientechnologies.orient.core.sql.filter.OSQLFilter

object SQLSurgery {
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
  import com.orientechnologies.orient.core.command.OBasicCommandContext
  import com.orientechnologies.orient.core.sql.filter.OSQLTarget
  import com.orientechnologies.orient.core.record.impl.ODocument
  import com.orientechnologies.orient.core.sql.OSQLHelper
  import com.orientechnologies.orient.core.sql.ORuntimeResult
  import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(1364); 

  //FIXME: Tune the hell out of these
  OGlobalConfiguration.CACHE_LEVEL1_SIZE.setValue(1000);$skip(56); 
  OGlobalConfiguration.CACHE_LEVEL2_SIZE.setValue(5000);$skip(47); 

  val uri = "local:/home/james/orientdb/test";System.out.println("""uri  : String = """ + $show(uri ));$skip(49); 
  implicit val db = new ODatabaseDocumentTx(uri);System.out.println("""db  : com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx = """ + $show(db ));$skip(348); val res$0 = 

  if (!db.exists) {
    db.create():ODatabaseDocumentTx
    val schema = db.getMetadata().getSchema()
    val sampleCls = schema.createClass("Sample")
    val prop = sampleCls.createProperty("timestamp", OType.DATETIME)
    prop.createIndex(INDEX_TYPE.NOTUNIQUE)
    //ensureSchema
  } else {
    db.open("admin","admin"): ODatabaseDocumentTx
  };System.out.println("""res0: Object = """ + $show(res$0));$skip(1434); 

  try {
    val context = new OBasicCommandContext()
    val target = "Sample"
    val filter = "url = 'http://localhost'"
    val projections = Seq("timestamp","url + '/' + url","responseTime + 1000")

    val parsedTarget = new OSQLTarget(target, context, null)
    val parsedFilter = new OSQLFilter(filter, context, null)
    val parsedProjections = projections map {proj => new OSQLFilter(proj, context, null)}

    val results = ((if (parsedTarget.getTargetClasses() != null) {
      for (clazz <- parsedTarget.getTargetClasses().values().view;
           doc <- (db.browseClass(clazz): Iterable[ODocument])) yield doc
    } else if (parsedTarget.getTargetRecords() != null) {
      parsedTarget.getTargetRecords().view.map (_.getRecord())
    } else if (parsedTarget.getTargetClusters() != null) {
      for (cluster <- parsedTarget.getTargetClusters().values().view;
           doc <- (db.browseCluster(cluster): Iterable[ODocument])) yield doc
    } else if (parsedTarget.getTargetIndex() != null) {
      db.getMetadata().getIndexManager().getIndex(parsedTarget.getTargetIndex()).getEntriesBetween(null, null): Iterable[ODocument]
    } else {
      List.empty[ODocument]
    }).filter {doc =>
      parsedFilter.evaluate(doc, doc, context) == java.lang.Boolean.TRUE
    }).map {doc =>
      parsedProjections map {proj => proj.evaluate(doc, doc, context)}
    }

    println(results.head)

  } finally {
    db.close()
  }}
}
