package uk.me.jamespic.dougng.model
import javax.persistence.{Id, Version}
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.impl.ODocument
import util._
import scala.collection.immutable.SortedMap
import scala.collection.mutable.{Map => MMap}
import java.util.Date

class Dataset {
  @Id var id: String = _
  var metric: String = _
  var rowName: String = _
  var timestamp: String = "timestamp"
  var table: String = "Sample"
  var whereClause: String = "true"

  @Version var version: String = _
}