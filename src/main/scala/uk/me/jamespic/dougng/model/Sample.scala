package uk.me.jamespic.dougng.model

import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.impl.ODocument
import java.util.{ArrayList, HashSet, Date, List => JList, Set => JSet}
import scala.collection.JavaConversions._
import util._
import com.orientechnologies.orient.core.id.ORID

/**
 * Samples are a better fit for the document database than the object database,
 * so there's no class for them. Instead, they're handled by Singleton methods
 * and pimping
 */
object Sample {
  // Somehow, using these aliases seems natural
  type Sample = ODocument
  type Counter = ODocument
  type Test = ODocument

  def Sample(name: String, ts: Date)(implicit db: ODatabaseDocument): Sample = {
    val doc: Sample = db.newInstance("Sample")
    doc("name") = name
    doc("timestamp") = ts
  }

  def Sample(name: String, ts: Date, success: Boolean, responseCode: Int)(implicit db: ODatabaseDocument): Sample = {
    val doc: Sample = Sample(name, ts)
    doc("success") = success
    doc("responseCode") = responseCode
  }

  def Sample(name: String, ts: Date, success: Boolean)(implicit db: ODatabaseDocument): Sample = {
    Sample(name, ts, success, if (success) 200 else 500)
  }

  def Sample(name: String, ts: Date, success: Boolean, responseCode: Int, url: String)(implicit db: ODatabaseDocument): Sample = {
    val doc: Sample = Sample(name, ts, success, responseCode)
    doc("responseCode") = responseCode
    doc("url") = url
  }

  def Test(name: String, startTime: Date)(implicit db: ODatabaseDocument) = {
    val doc: Test = db.newInstance("DataSource")
    doc("name") = name
    doc("startTime") = startTime
  }

  implicit class SamplePimp(val doc: Sample) extends AnyVal {
    def +=(child: Sample) = {
      child.save()
      doc.save()
      child("parent") = doc.getIdentity()
      if (doc("children") == null) {
        doc("children") = new ArrayList[Sample]()
      }
      (doc("children"): JList[Sample]) += child
    }

    def counters = new Counters(doc)
  }

  class Counters(val doc: Sample) extends AnyVal {
    def update(name: String, value: Double)(implicit db: ODatabaseDocument) = {
      doc.save()
      if (doc("counters") == null) {
        doc("counters") = new HashSet[Counter]()
      }
      val counter: Counter = db.newInstance("Counter")
      counter("name") = name
      counter("value") = value
      counter("parent") = doc.getIdentity()
      counter.save()
      (doc("counters"): JSet[Counter]) += counter
      counter
    }
  }
}