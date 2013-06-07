package uk.me.jamespic.dougng.util

object MapReduce {
  def sum[A](trav: Traversable[A])(implicit num: Numeric[A]): Option[A] = {
    trav.reduceOption[A]((x, y) => num.mkNumericOps(x) + y) // Stupid string implicits
  }
}