package uk.me.jamespic.dougng

import org.scalatest.Suite

trait DBMixin extends EarlyDBMixin with OrientMixin

private[dougng] trait EarlyDBMixin extends Suite {this: OrientMixin =>
  abstract override def withFixture(test: NoArgTest) = {
    import model.util._
    ensureSchema(db)
    registerClasses(db)
    super.withFixture(test)
  }
}