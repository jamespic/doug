package uk.me.jamespic.dougng.model

import org.scalatest.{FunSpecLike, Matchers}
import uk.me.jamespic.dougng.OrientMixin
import uk.me.jamespic.dougng.model.util.ScalaObjectMethodFilter

class ScalaObjectMethodFiterTest extends FunSpecLike with Matchers with OrientMixin {
  describe("A ScalaObjectMethodFilter") {
    it("should recognise valid getter methods") {
      val m = cls.getMethod("testVar")
      ScalaObjectMethodFilter.getFieldName(m) should equal("testVar")
      ScalaObjectMethodFilter.isGetterMethod("testVar", m) should be(true)
      ScalaObjectMethodFilter.isSetterMethod("testVar", m) should be(false)
      ScalaObjectMethodFilter.isHandled(m) should be(true)
    }

    it("should recognise valid setter methods") {
      val m = cls.getMethod("testVar_$eq", classOf[String])
      ScalaObjectMethodFilter.getFieldName(m) should equal("testVar")
      ScalaObjectMethodFilter.isGetterMethod("testVar_$eq", m) should be(false)
      ScalaObjectMethodFilter.isSetterMethod("testVar_$eq", m) should be(true)
      ScalaObjectMethodFilter.isHandled(m) should be(true)
    }

    it("should refuse to recognise getters with parameters") {
      val m = cls.getMethod("badGetter", classOf[String])
      ScalaObjectMethodFilter.isGetterMethod("badGetter", m) should be(false)
      ScalaObjectMethodFilter.isSetterMethod("badGetter", m) should be(false)
      ScalaObjectMethodFilter.isHandled(m) should be(false)
    }

    it("should refuse to recognise setters with two parameters") {
      val m = cls.getMethod("badSetter_$eq", classOf[String], classOf[String])
      ScalaObjectMethodFilter.isGetterMethod("badSetter_$eq", m) should be(false)
      ScalaObjectMethodFilter.isSetterMethod("badSetter_$eq", m) should be(false)
      ScalaObjectMethodFilter.isHandled(m) should be(false)
    }

    it("should refuse to handle getters if there is no corresponding field") {
      val m = cls.getMethod("fakeProp")
      ScalaObjectMethodFilter.isHandled(m) should be(false)
    }

    it("should refuse to handle setters if there is no corresponding field") {
      val m = cls.getMethod("fakeProp_$eq", classOf[String])
      ScalaObjectMethodFilter.isHandled(m) should be(false)
    }
  }

  class TestPojo {
    var testVar: String = "Hello"
    def badGetter(a: String) = a
    def badSetter_=(a: String, b: String) = a + b
    def fakeProp = "Hello"
    def fakeProp_=(v: String) = {}
  }

  val cls = classOf[TestPojo]

}