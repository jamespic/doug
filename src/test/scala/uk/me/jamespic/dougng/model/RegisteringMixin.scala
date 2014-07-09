package uk.me.jamespic.dougng.model

import uk.me.jamespic.dougng.OrientMixin

trait RegisteringMixin extends OrientMixin {
  abstract override def withFixture(f: NoArgTest): org.scalatest.Outcome = {
    super.withFixture(new NoArgTest {
      val configMap = f.configMap
      val name = f.name
      val scopes = f.scopes
      val tags = f.tags
      val text = f.text
      def apply() = {
        util.initDB
        if (db.getEntityManager.getRegisteredEntities.size < 6) {
          println(s"*** Initialisation failed: Current entities: ${db.getEntityManager.getRegisteredEntities}")
        }
        try f()
        finally util.uninitDB
      }
    })
  }
}