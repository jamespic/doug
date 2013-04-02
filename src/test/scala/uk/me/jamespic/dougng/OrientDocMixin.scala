package uk.me.jamespic.dougng

import org.scalatest.Suite
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx

trait OrientDocMixin extends Suite { this: OrientMixin =>
  implicit var docDb: ODatabaseDocumentTx = _
  abstract override def withFixture(test: NoArgTest) {
    try {
      docDb = new ODatabaseDocumentTx(dbUri)
      docDb = docDb.open("admin", "admin")
      super.withFixture(test)
    } finally {
      if (docDb != null) {
        docDb.close()
        docDb = null
      }
    }
  }
}