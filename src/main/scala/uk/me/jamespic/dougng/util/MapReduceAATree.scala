package uk.me.jamespic.dougng.util

object MapReduceAATree {
  def empty[K, V, S](mapReduce: Traversable[V] => Option[S],
      rereduce: Traversable[S] => Option[S])(implicit ord: Ordering[K]): MapReduceAATree[K, V, S] = {
    val ctx = new TreeContext[K, V, S](mapReduce, rereduce)
    ctx.NullNode
  }
  private[util] def assert(err: => String)(check: => Boolean) = {
    if (!check) throw new AssertionError(s"Assertion failed: $err")
  }
}

sealed trait MapReduceAATree[K, V, S] extends Traversable[(K, V)] {
  private[util] val level: Int
  val summary: Option[S]
  def +(e: (K, V)): MapReduceAATree[K, V, S]
  def checkInvariants: Unit
  def summaryBetween(from: K, to: K): Option[S]
  def summaryBetween(from: Option[K], to: Option[K]): Option[S]
  override def stringPrefix = "MapReduceAATree"
}

private[util] final class TreeContext[K, V, S]
    (mapReduce: Traversable[V] => Option[S], rereduce: Traversable[S] => Option[S])
    (implicit ord: Ordering[K]){
  import MapReduceAATree._
  import Ordering.Implicits._
  type Node = MapReduceAATree[K, V, S]

  private[util] case object NullNode extends Node {
    val summary = None
    val level = 0
    def checkInvariants = {}
    override def foreach[U](f: ((K, V)) => U) = {}
    override def isEmpty = true
    override def +(e: (K, V)) = {
      val (key, value) = e
      BranchNode(key, List(value), NullNode, NullNode, 1)
    }
    override def summaryBetween(from: K, to: K) = None
    override def summaryBetween(from: Option[K], to: Option[K]) = None
  }

  private[util] case class BranchNode(key: K, values: List[V], left: Node, right: Node, level: Int) extends Node {
    override def foreach[U](f: ((K, V)) => U) = {
      left.foreach(f)
      for (value <- values) f((key, value))
      right.foreach(f)
    }

    override def isEmpty = false

    lazy val summary: Option[S] = {
      rereduce(
        Traversable.concat(
          mapReduce(values),
          left.summary,
          right.summary
        )
      )
    }

    override def summaryBetween(from: K, to: K) = {
      summaryBetween(Some(from), Some(to))
    }

    override def summaryBetween(from: Option[K], to: Option[K]) = {
      if (from == None && to == None) summary
      else {
        val leftRightWall = to filter (_ < key)
        val leftBlock = from match {
          case Some(fromVal) if fromVal < key =>
            left.summaryBetween(from, leftRightWall)
          case None =>
            left.summaryBetween(None, leftRightWall)
          case _ =>
            None
        }

        val rightLeftWall = from filter (_ > key)
        val rightBlock = to match {
          case Some(toVal) if toVal > key =>
            right.summaryBetween(rightLeftWall, to)
          case None =>
            right.summaryBetween(rightLeftWall, to)
          case _ =>
            None
        }

        val centreBlock = if (from.exists(_ > key) || to.exists(_ < key)) {// if from > key or to < key, don't include values
          None
        } else {
          mapReduce(values)
        }
        rereduce(Traversable.concat(leftBlock, centreBlock, rightBlock))
      }
    }

    override def +(e: (K, V)) = {
      val (newKey, value) = e
      ord.compare(newKey, key) match {
        case -1 =>
          val newLeft = left + e
          BranchNode(key, values, newLeft, right, level).balance
        case 1 =>
          val newRight = right + e
          BranchNode(key, values, left, newRight, level).balance
        case 0 =>
          BranchNode(key, value :: values, left, right, level)
      }
    }

    def skew = left match {
      case BranchNode(lKey, lValues, a, b, lLevel) if lLevel == this.level =>
        val t = BranchNode(this.key, this.values, b, this.right, this.level)
        val l = BranchNode(lKey, lValues, a, t, lLevel)
        l
      case _ => this
    }

    def split = right match {
      case BranchNode(rKey, rValues, b, x, rLevel) if x.level == this.level =>
        val t = BranchNode(this.key, this.values, this.left, b, this.level)
        val r = BranchNode(rKey, rValues, t, x, rLevel + 1)
        r
      case _ => this
    }

    def balance = this.skew.split

    def checkInvariants = {
      assert("Left child is below me")(left.level == this.level - 1)
      assert("Right child is level with or below me")(right.level == this.level || right.level == this.level - 1)
      right match {
        case BranchNode(k, _, _, r2, _) =>
          assert("Right grandchild is lower than me")(r2.level < this.level)
          assert("Right is more than me")(k > this.key)
        case _ => // Do nothing
      }
      left match {
        case l: BranchNode => assert("Left is less than me")(l.key < this.key)
        case _ => //Do nothing
      }
      if (left == NullNode && right == NullNode) {
        assert("I'm a leaf node, with level 1")(level == 1)
      }
      if (level > 1) {
        assert("I have 2 children") {
          right.isInstanceOf[BranchNode] && left.isInstanceOf[BranchNode]
        }
      }
      left.checkInvariants
      right.checkInvariants
    }
  }
}