package uk.me.jamespic.dougng.viewmodel

import org.scalatest.FunSpec
import uk.me.jamespic.dougng.model.RegisteringMixin
import org.scalatest.GivenWhenThen
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import com.orientechnologies.orient.core.record.impl.ODocument
import scala.collection.JavaConversions._
import uk.me.jamespic.dougng.model.TimeGraph
import java.awt.Color
import scala.collection.SortedMap


class JSONTest extends FunSpec with Matchers with RegisteringMixin
    with GivenWhenThen with BeforeAndAfterAll with BeforeAndAfter {
  describe("A JSON object") {
    it("should serialize plain json objects") {
      val instance = new JsonSerializer(db)
      instance.dumps(1) should equal("1")
      instance.dumps("Hello World") should equal("\"Hello World\"")
      instance.dumps(1L) should equal("1")
      instance.dumps(1.2) should equal("1.2")
      instance.dumps(true) should equal("true")
      instance.dumps(1.toShort) should equal("1")
      instance.dumps(BigDecimal("1.6")) should equal("1.6")
      instance.dumps(null) should equal("null")
    }

    it("should serialize Maps to Json objects") {
      val instance = new JsonSerializer(db)
      instance.dumps(Map("Hello" -> "World")) should equal("{\"Hello\": \"World\"}")
      instance.dumps(scala.collection.mutable.Map("Hello" -> 1)) should equal("{\"Hello\": 1}")
      instance.dumps(SortedMap(true -> BigDecimal(1))) should equal("{true: 1}")
    }

    it("should serialize Traversables to Json objects") {
      val instance = new JsonSerializer(db)
      instance.dumps(Seq("Hello", "World")) should equal("[\"Hello\", \"World\"]")
      instance.dumps(Traversable("Hello", "World")) should equal("[\"Hello\", \"World\"]")
      instance.dumps(Iterator("Hello", "World")) should equal("[\"Hello\", \"World\"]")
      instance.dumps(Array("Hello", "World")) should equal("[\"Hello\", \"World\"]")
      instance.dumps(Set("Hello", "World")) should equal("[\"Hello\", \"World\"]")
    }

    it("should serialize case classes") {
      val instance = new JsonSerializer(db)
      instance.dumps(TestClass(1)) should equal("{\"class\": \"TestClass\", \"a\": 1}")
      instance.dumps(Some(1)) should equal("{\"class\": \"Some\", \"x\": 1}")
      instance.dumps(None) should equal("{\"class\": \"None$\"}")
    }

    it("should serialize ODocuments") {
      val instance = new JsonSerializer(db)
      val doc = docDb.newInstance("TestDBClass")
      doc.field("name", "MyName")
      doc.field("value", 1L)
      doc.field("data", new java.util.concurrent.CopyOnWriteArrayList(Array("Hello","World")))
      doc.field("child", (new ODocument).field("data",1))
      doc.save()
      instance.dumps(doc) should fullyMatch regex """\{"@rid":"#\d+:\d+","@version":0,"@class":"TestDBClass","name":"MyName","value":1,"data":\["Hello","World"\],"child":\{"@rid":"#\d+:\d+","@version":0,"data":1\},"@fieldTypes":"value=l"\}"""
    }

    it("should serialize registered POJOs") {
      val instance = new JsonSerializer(db)
      var timegraph = new TimeGraph
      timegraph.granularity = 20000
      timegraph.hiddenRows.add("Row1")
      timegraph.name = "MyTimeGraph"
      timegraph.rowColours.put("Row1", "#f00")
      timegraph = db.save(timegraph)
      instance.dumps(timegraph) should fullyMatch regex("""\{"@rid":"#\d+:\d+","@version":0,"@class":"TimeGraph","granularity":20000,"maxDatasets":\[\],"datasets":\[\],"hiddenRows":\["Row1"\],"rowColours":\{"Row1":"#f00"\},"name":"MyTimeGraph"\}""")
    }

    it("should refuse to serialize anything else") {
      val instance = new JsonSerializer(db)
      intercept[MatchError] {
        instance.dumps(new Object)
      }
      intercept[MatchError] {
        instance.dumps(())
      }
      intercept[MatchError] {
        instance.dumps(Map("label" -> new Object))
      }
      intercept[MatchError] {
        instance.dumps(instance)
      }
      intercept[MatchError] {
        instance.dumps(new Exception)
      }
    }
  }
}

case class TestClass(a: Int)