package uk.me.jamespic.dougng.model
import javax.persistence.{Id, Version}
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.impl.ODocument
import util._
import scala.collection.immutable.SortedMap
import scala.collection.mutable.{Map => MMap}
import java.util.Date
import uk.me.jamespic.dougng.util.Average

object Dataset {
  def mapToSortedMap[A, B, C](map: MMap[A, B], f: B => C)(implicit ord: Ordering[A]) = {
    val builder = SortedMap.newBuilder[A, C]
    for ((k, v) <- map) {
      builder += k -> f(v)
    }
    builder.result
  }
}

class Dataset {
  @Id var id: String = _
  var metric: String = _
  var rowName: String = _
  var timestamp: String = "timestamp"
  var table: String = "Sample"
  var whereClause: String = _
  @Version var version: String = _

  // TODO: Add support for adjusting time by Test.startTime
  def queryString = s"""select timestamp,
                            $metric as metric,
                            $rowName as rowName
                        from $table
                        where ($whereClause)"""
                          //and ($metric is not null)
                          //and ($rowName is not null)"""

  def foldByTime[S](db: ODatabaseDocument, granularity: Long, default: => S)(foldFun: (S, Double) => S) = {
    val start = MMap.empty[String, MMap[Long, S]]
    (start /: db.asyncSql(queryString)){(m, d) =>
      val ts = d[Date]("timestamp").getTime
      val window = ts - ts % granularity
      val rowName: String = d("rowName")
      val metric: Number = d("metric")
      if (rowName != null && metric != null) {
        val row = m.getOrElseUpdate(rowName, MMap.empty[Long, S])
        val data = row.getOrElse(window, default)
        row(window) = foldFun(data, metric.doubleValue)
      }
      m
    }
  }

  def maxData(db: ODatabaseDocument, granularity: Long) = {
    import Dataset._
    val baseCollection = foldByTime(db, granularity, Double.MinValue)(_ max _)
    mapToSortedMap(baseCollection, {(map: MMap[Long, Double]) => mapToSortedMap(map, (x: Double) => x)})
  }

  def avgData(db: ODatabaseDocument, granularity: Long) = {
    import Dataset._
    val baseCollection = foldByTime[Average](db, granularity, Average)(_ + _)
    mapToSortedMap(baseCollection, {(map: MMap[Long, Average]) => mapToSortedMap(map, (_: Average).avg)})
  }
}