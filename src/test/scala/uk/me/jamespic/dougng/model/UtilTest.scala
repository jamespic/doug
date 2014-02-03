package uk.me.jamespic.dougng.model

import org.scalatest.{FunSpecLike, Matchers, GivenWhenThen}
import uk.me.jamespic.dougng.OrientMixin
import uk.me.jamespic.dougng.model.util._
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.impl.ODocument
import java.util.{List => JList}
import scala.collection.JavaConversions._

class UtilTest extends FunSpecLike with Matchers with OrientMixin with GivenWhenThen {

  def populate(docDb: ODatabaseDocument) {
    val schema = docDb.getMetadata().getSchema()
    if (!schema.existsClass("TestClass")) {
      schema.createClass("TestClass")
      val doc1 = docDb.newInstance[ODocument]("TestClass")
      doc1.field("number", 1)
      doc1.save()
      val doc2 = docDb.newInstance[ODocument]("TestClass")
      doc2.field("number", 2)
      doc2.save()
    }
  }

  describe("The ensureSchema method") {
    it("should create a schema") {
      ensureSchema
      registerClasses
      val graphClass = db.browseClass(classOf[Graph])
      graphClass.headOption should be(None)
    }
  }

  describe("A pimped-out ODatabaseDocumentTx") {
    it("should provide a traversable view of a query") {
      Given("a document database")
      val docDb = db.getUnderlying()
      When("populated")
      populate(docDb)
      And("queried, via pimping")
      val traversable = docDb.asyncSql("select from TestClass")
      val listed = ((Nil: List[ODocument]) /: traversable){(l, d) => l :+ d}
      Then("the result should have length 2")
      listed should have size(2)
      info(s"List produced was $listed")
    }
  }

  describe("The lfloor function") {
    it("should round positive longs down") {
      (121L floor 30L) should equal (120L)
      (119L floor 30L) should equal (90L)
    }

    it("should leave divisible longs unchanged") {
      (150L floor 30L) should equal (150L)
      (-150L floor 30L) should equal (-150L)
    }

    it("should round negative longs down") {
      (-121L floor 30L) should equal (-150L)
      (-119L floor 30L) should equal (-120L)
    }
  }
}