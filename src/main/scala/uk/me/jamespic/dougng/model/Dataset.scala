package uk.me.jamespic.dougng.model
import javax.persistence.{Id, Version}
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.{OCommandExecutorSQLSelect, OCommandSQL}
import util._
import scala.collection.immutable.SortedMap
import scala.collection.mutable.{Map => MMap}
import java.util.Date

import scala.util.control.NonFatal

class Dataset {
  @Id var id: String = _
  var name: String = ""
  var query: String = _

  @Version var version: String = _
}