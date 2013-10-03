package uk.me.jamespic.dougng.model.datamanagement

import com.orientechnologies.orient.`object`.db._
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.record.ODatabaseRecord

object ReplacablePool {
  private val DefaultUserName = "admin"
  private val DefaultPassword = "admin"
}

class ReplacablePool {
  import ReplacablePool._
  private var pool: Option[OObjectDatabasePool] = None
  private var _url: String = null

  private[dougng] def clear = synchronized {
    pool.foreach(_.close)
    pool = None
    _url = null
  }

  private[dougng] def url_=(u: String) = synchronized {
    clear
    pool = Some(new OObjectDatabasePool(u, DefaultUserName, DefaultPassword))
    _url = u
  }

  private[dougng] def url = synchronized(_url)

  def map[X](f: OObjectDatabaseTx => X) = {
    val db = pool.get.acquire
    try {
      f(db)
    } finally {
      db.close
    }
  }

  def foreach[U](f: OObjectDatabaseTx => U): Unit = map(f)
}