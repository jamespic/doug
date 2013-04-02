package uk.me.jamespic.dougng.model
import com.orientechnologies.orient.core.metadata.schema.{OSchema, OType, OClass}
import com.orientechnologies.orient.core.command.OCommandResultListener
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery
import com.orientechnologies.orient.`object`.enhancement.OObjectEntityEnhancer
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import com.orientechnologies.orient.core.db.ODatabaseComplex
import com.orientechnologies.orient.core.metadata.schema.OProperty

package object util {
  val modelClasses = Seq(classOf[Dataset],
                         classOf[Graph],
                         classOf[RowGraph],
                         classOf[TimeGraph])

  def ensureSchema(implicit db: ODatabaseComplex[_]) {
    import OType._
    val schema = db.getMetadata().getSchema()

    def ensureClass(clsName: String, superClass: OClass = null)(f: OClass => Unit) = {
      if (!schema.existsClass(clsName)) {
        val cls = if (superClass == null) {
          schema.createClass(clsName)
        } else {
          schema.createClass(clsName, superClass)
        }
        f(cls)
        cls
      } else {
        schema.getClass(clsName)
      }
    }

    def indexedProp(cls: OClass, propName: String, propType: OType) = {
      val prop = cls.createProperty(propName, propType)
      //prop.setMandatory(true)
      //prop.setNotNull(true)
      val idxName = s"idx_${cls.getName}_$propName"
      cls.createIndex(idxName, OClass.INDEX_TYPE.NOTUNIQUE, propName)
      prop
    }

    implicit class PropReq(val prop: OProperty) {
      def required = prop.setMandatory(true).setNotNull(true)
    }

    val testCls = ensureClass("Test") {cls =>
      indexedProp(cls, "name", STRING).required
      cls.createProperty("startTime", DATETIME)
    }

    val sampleCls = ensureClass("Sample") {cls =>
      indexedProp(cls, "timestamp", DATETIME).required
      indexedProp(cls, "name", STRING).required
      indexedProp(cls, "success", BOOLEAN)
      indexedProp(cls, "responseCode", INTEGER)
      indexedProp(cls, "url", STRING)
      cls.createProperty("parent", LINK, cls)
      cls.createProperty("children", LINKLIST, cls)
      cls.createProperty("test", LINK, testCls)
    }

    val counterCls = ensureClass("Counter") {cls =>
      indexedProp(cls, "name", STRING).required

      cls.createProperty("parent", LINK, sampleCls)
      cls.createProperty("value", DOUBLE).required

      sampleCls.createProperty("counters", LINKSET, cls)
    }
  }

  def registerClasses(implicit db: OObjectDatabaseTx) {
    for (cls <- modelClasses) {
      registerClass(cls)
    }
  }

  def registerClass(cls: Class[_])(implicit db: OObjectDatabaseTx) = {
    OObjectEntityEnhancer.getInstance().registerClassMethodFilter(cls, ScalaObjectMethodFilter)
    val regCls = db.getEntityManager().registerEntityClass(cls)
  }

  class QueryTraversable[A](handler: OCommandResultListener => Unit) extends Traversable[A] {
    override def foreach[U](f: A => U) = {
      handler(new OCommandResultListener {
        override def result(iRecord: Object) = {
          f(iRecord.asInstanceOf[A])
          true
        }
      })
    }
  }

  implicit class ObjectDBPimp(val db: ODatabaseDocument) extends AnyVal {
    def asyncSql(sql: String) = {
      new QueryTraversable[ODocument]({listener =>
        db.query(new OSQLAsynchQuery(sql, listener))
        ()
      })
    }
  }

  implicit class DocumentPimp(val rec: ODocument) extends AnyVal {
    def apply[RET](key: String) = rec.field[RET](key)
    def update(key: String, value: Any) = rec.field(key, value)
  }

  implicit class LongPimp(val n: Long) extends AnyVal {
    def floor(d: Long) = {
      val rem = n % d
      n - (if (rem >= 0) rem else rem + d)
    }
  }
}