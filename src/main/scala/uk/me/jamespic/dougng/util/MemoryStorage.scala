package uk.me.jamespic.dougng.util

import scala.collection.mutable.ArrayBuffer

private[util] class MemoryStorage[K, S](val combiner: (S, S) => S)(implicit val ordering: Ordering[K]) extends MapReduceStorage[K, S] {
  private[util] val storage = ArrayBuffer.empty[(K, S)]
  def length: Long = storage.size
  def keyAt(i: Long): K = storage(i.toInt)._1
  def summaryAt(i: Long): S = storage(i.toInt)._2
  def apply(i: Long): (K, S) = storage(i.toInt)
  def foreach[U](f: ((K, S)) => U) {
    storage.foreach(f)
  }
}

class MemoryBuilder[K, S] (val combiner: (S, S) => S,  val fanout: Int)(implicit val ordering: Ordering[K]) extends MapReduceBuilder[K, S] {
  val storage = new MemoryStorage(combiner)
  var finished = false

  override def +=(e: (K, S)) = {
    checkFinished()
    super.+=(e)
  }

  def persist(e: (K, S)) {
    checkFinished()
    storage.storage += e
  }

  private def checkFinished() {
    if (storage.size == Integer.MAX_VALUE || finished) {
      throw new IllegalStateException(s"MemoryStorage has a max capacity of Integer.MAX_VALUE")
    }
  }

  def finish = {
    head match {
      case Some(e) =>
        persist(e)
        head = None
      case _ => ()
    }

    finished = true

    storage
  }
}