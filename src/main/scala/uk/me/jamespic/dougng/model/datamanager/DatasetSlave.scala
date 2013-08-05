package uk.me.jamespic.dougng.model.datamanager

import akka.actor.Actor
import com.orientechnologies.orient.core.command.OBasicCommandContext
import com.orientechnologies.orient.core.sql.filter.{OSQLTarget, OSQLFilter}
import scala.collection.JavaConversions._
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.command.OCommandContext
import scala.collection.mutable.{Map => MMap}

class DatasetSlave(db: ODatabaseDocumentTx) extends Actor {
  def receive = {
    case UpdateDatasets(tableName, datasets) =>
      val context = new OBasicCommandContext()
      val browser = browse(tableName, context)

      def compile(cmd: String) = new OSQLFilter(cmd, context, null)
      val compiled = for (dataset <- datasets) yield
        (dataset.id, Compiled(compile(dataset.whereClause),
            compile(dataset.rowName),
            compile(dataset.timestamp),
            compile(dataset.metric)))

      for {doc <- browser
           (id, Compiled(filter, nameComp, tsComp, metricComp)) <- compiled
           if filter.evaluate(doc, doc, context) == java.lang.Boolean.TRUE} {

        val rowName = nameComp.evaluate(doc, doc, context).toString

        val ts = tsComp.evaluate(doc, doc, context) match {
          case l: java.lang.Long => l.longValue
          case d: java.util.Date => d.getTime()
        }

        val metric = metricComp.evaluate(doc, doc, context) match {
          case n: java.lang.Number => n.doubleValue
        }
      }

      val result = ???

      sender ! DatasetsUpdated(result)
  }

  private def browse(target: String, context: OCommandContext) = {
    val parsed = new OSQLTarget(target, context, null)

    val browser = {
      if (parsed.getTargetClasses() != null) {
        for (clazz <- parsed.getTargetClasses().values().view;
             doc <- (db.browseClass(clazz): Iterable[ODocument])) yield doc
      } else if (parsed.getTargetRecords() != null) {
        parsed.getTargetRecords().view.map (_.getRecord())
      } else if (parsed.getTargetClusters() != null) {
        for (cluster <- parsed.getTargetClusters().values().view;
             doc <- (db.browseCluster(cluster): Iterable[ODocument])) yield doc
      } else if (parsed.getTargetIndex() != null) {
        db.getMetadata().getIndexManager().getIndex(parsed.getTargetIndex()).getEntriesBetween(null, null): Iterable[ODocument]
      } else {
        List.empty[ODocument]
      }
    }

    browser
  }

  private case class Compiled(filter: OSQLFilter,
                              name: OSQLFilter,
                              timestamp: OSQLFilter,
                              metric: OSQLFilter)
}