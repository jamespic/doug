package uk.me.jamespic.dougng.util

import scala.collection.{LinearSeq, SortedMap}

trait MapReduceBuilder[K, S] {
  val ordering: Ordering[K]
  val fanout: Int
  val combiner: (S, S) => S

  lazy val blockSize = 1L << fanout
  protected var head: Option[(K, S)] = None
  protected var counter = 0
  protected var last: Option[K] = None

  def +=(e: (K, S)) = {
    val (key, sum) = e
    checkSequence(key)

    if (counter == blockSize) {
      persist(head.get)
      head = None
      counter = 0
    }

    head = head match {
      case Some((oldKey, oldSum)) =>  Some((oldKey, combiner(oldSum, sum)))
      case None => Some(key, sum)
    }

    counter += 1
  }

  private def checkSequence(k: K) {
    if (last.exists(l => ordering.lt(l, k))) {
      throw new IllegalArgumentException("New key must be bigger than last key added")
    }
  }

  def finish: MapReduceStorage[K, S]
  protected def persist(e: (K, S)): Unit
}

trait MapReduceStorage[K, S] extends Traversable[(K, S)] {
  val ordering: Ordering[K]
  val combiner: (S, S) => S
  def length: Long
  def keyAt(i: Long): K
  def summaryAt(i: Long): S
  def apply(i: Long): (K, S)

  def findIndex(k: K): Either[Long, Long] = findIndex(k, 0, length - 1)

  private def findIndex(k: K, from: Long, to: Long): Either[Long, Long] = {
    val key = ordering.mkOrderingOps(k)
    if (key > keyAt(to)) {
      Right(to + 1)
    } else if (key < keyAt(from)) {
      Right(from)
    } else {
      val mid = (from + to) / 2
      val midKey = keyAt(mid)
      ordering.compare(k, midKey) match {
        case -1 => findIndex(k, from, mid - 1)
        case 0 => Left(mid)
        case 1 => findIndex(k, mid + 1, to)
      }
    }
  }
}