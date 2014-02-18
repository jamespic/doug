package uk.me.jamespic.dougng.model.datamanagement

import com.orientechnologies.orient.`object`.db._
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.record.ODatabaseRecord
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.Lock

object ReplacablePool {
  private val DefaultUserName = "admin"
  private val DefaultPassword = "admin"
  def apply(url: String) = {
    val pool = new ReplacablePool
    pool.url = url
    pool
  }
}

class ReplacablePool {
  import ReplacablePool._
  private var pool: Option[OObjectDatabasePool] = None
  private var _url: String = null
  private val lock = new ReentrantReadWriteLock

  private[dougng] def clear = withWriteLock {
    pool.foreach(_.close)
    pool = None
    _url = null
  }

  private[dougng] def url_=(u: String) = withWriteLock {
    clear
    pool = Some(new OObjectDatabasePool(u, DefaultUserName, DefaultPassword))
    _url = u
  }

  private[dougng] def url = withReadLock(_url)

  def map[X](f: OObjectDatabaseTx => X) = withReadLock {
    val db = pool.get.acquire
    try {
      f(db)
    } finally {
      db.close
    }
  }

  def flatMap[X](f: OObjectDatabaseTx => X) = map(f)

  private def withWriteLock[X] = withLock[X](lock.writeLock) _
  private def withReadLock[X] = withLock[X](lock.writeLock) _

  private def withLock[X](l: Lock)(f: => X) = scala.concurrent.blocking {
    l.lock()
    try f
    finally l.unlock()
  }

  def foreach[U](f: OObjectDatabaseTx => U): Unit = map(f)
}