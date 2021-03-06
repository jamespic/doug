package uk.me.jamespic.dougng.model
import com.orientechnologies.orient.core.metadata.schema.{OSchema, OType, OClass}
import com.orientechnologies.orient.core.command.{OCommandRequest, OCommandResultListener}
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery
import com.orientechnologies.orient.`object`.enhancement.OObjectEntityEnhancer
import com.orientechnologies.orient.`object`.db.{OObjectDatabaseTxPooled, OObjectDatabasePool, OObjectDatabaseTx}
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

    implicit class PropReq(val prop: OProperty) {
      def required = prop.setMandatory(true).setNotNull(true)
    }

    val testCls = ensureClass("Test") {cls =>
      cls.createProperty("startTime", DATETIME).required
      cls.createProperty("name", STRING)
    }

    val sampleCls = ensureClass("Sample") {cls =>
      cls.createProperty("timestamp", DATETIME).required
      cls.createProperty("name", STRING)
      cls.createProperty("success", BOOLEAN)
      cls.createProperty("responseCode", INTEGER)
      cls.createProperty("url", STRING)
      cls.createProperty("parent", LINK, cls)
      cls.createProperty("children", LINKLIST, cls)
      cls.createProperty("test", LINK, testCls)
    }

    val counterCls = ensureClass("Counter") {cls =>
      cls.createProperty("parent", LINK, sampleCls)
      cls.createProperty("name", STRING).required
      cls.createProperty("value", DOUBLE).required
      sampleCls.createProperty("counters", LINKSET, cls)
    }
  }

  def initDB(implicit db: OObjectDatabaseTx) {
    db.setAutomaticSchemaGeneration(true)
    registerClasses
    ensureSchema
  }

  def uninitDB(implicit db: OObjectDatabaseTx) {
    deregisterClasses
  }

  def registerClasses(implicit db: OObjectDatabaseTx) {
    for (cls <- modelClasses) {
      registerClass(cls)
    }
  }

  def deregisterClasses(implicit db: OObjectDatabaseTx) {
    for (cls <- modelClasses) {
      deregisterClass(cls)
    }
  }

  def registerClass(cls: Class[_])(implicit db: OObjectDatabaseTx) = {
    OObjectEntityEnhancer.getInstance().registerClassMethodFilter(cls, ScalaObjectMethodFilter)
    db.getEntityManager().registerEntityClass(cls)
  }

  def deregisterClass(cls: Class[_])(implicit db: OObjectDatabaseTx) = {
    db.getEntityManager().deregisterEntityClass(cls)
  }

  class QueryTraversable[A](handler: OCommandResultListener => Unit) extends Traversable[A] {
    override def foreach[U](f: A => U) = {
      handler(new OCommandResultListener {
        override def result(iRecord: Object) = {
          f(iRecord.asInstanceOf[A])
          true
        }
        override def end = ()
      })
    }
  }

  implicit class ObjectDBPimp(val db: ODatabaseDocument) extends AnyVal {
    def asyncSql(sql: String) = {
      new QueryTraversable[ODocument]({listener =>
        new OSQLAsynchQuery(sql, listener).execute(): Any
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

  implicit class PoolPimp(val pool: OObjectDatabasePool) extends AnyVal {
    def map[X](f: OObjectDatabaseTx => X) = {
      val db = pool.acquire()
      try {
        f(db)
      } finally db.close()
    }

    def flatMap[X](f: OObjectDatabaseTx => X) = map(f)
    def foreach[U](f: OObjectDatabaseTx => U): Unit = map(f)
  }
}