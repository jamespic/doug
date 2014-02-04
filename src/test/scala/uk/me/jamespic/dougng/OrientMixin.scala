package uk.me.jamespic.dougng

import org.scalatest.Suite
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import java.util.UUID

trait OrientMixin extends OrientDocMixin {
  implicit var db: OObjectDatabaseTx = _
  var dbUri: String = _
  abstract override def withFixture(test: NoArgTest): org.scalatest.Outcome = {
    try {
      dbUri = s"memory:${UUID.randomUUID}"
      db = new OObjectDatabaseTx(dbUri)
      if (!db.exists()) {
        db = db.create()
      } else {
        db = db.open("admin", "admin")
      }
      super.withFixture(test)
    } finally {
      if (db != null) {
        db.drop()
        db.close()
        db = null
      }
    }
  }
}