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

sealed trait MapReduceAATree[K, V, S] extends ImmutableMapReduce[K, V, S] {
  type SelfType = MapReduceAATree[K, V, S]
  private[util] val level: Int
  def checkInvariants: Unit
}

private[util] final class TreeContext[K, V, S]
    (mapReduce: Traversable[V] => Option[S], rereduce: Traversable[S] => Option[S])
    (implicit val ordering: Ordering[K]){
  import MapReduceAATree._
  import Ordering.Implicits._
  type Node = MapReduceAATree[K, V, S]

  private[util] case object NullNode extends Node {
    override val summary = None
    val level = 0
    implicit def ordering = TreeContext.this.ordering
    def checkInvariants = {}
    override def isEmpty = true
    def +(e: (K, V)) = {
      val (key, value) = e
      BranchNode(key, value :: Nil, NullNode, NullNode, 1)
    }
    def summaryBetween(low: Option[K], high: Option[K]) = None
    override def getBetween(low: Option[K], high: Option[K]) = Traversable.empty
    def doBetween[U](low: Option[K], high: Option[K], f: ((K, V)) => U) = ()
    def minKey = None
    def maxKey = None
  }

  private[util] case class BranchNode(key: K, values: List[V], left: Node, right: Node, level: Int) extends Node {
    override def foreach[U](f: ((K, V)) => U) = {
      left.foreach(f)
      for (value <- values) f((key, value))
      right.foreach(f)
    }

    implicit def ordering = TreeContext.this.ordering

    override def isEmpty = false

    override lazy val summary: Option[S] = {
      rereduce(
        Traversable.concat(
          mapReduce(values),
          left.summary,
          right.summary
        )
      )
    }

    def summaryBetween(low: Option[K], high: Option[K]) = {
      if (low == None && high == None) summary
      else {
        val leftRightWall = high filter (_ < key)
        val leftBlock = low match {
          case Some(lowVal) if lowVal < key =>
            left.summaryBetween(low, leftRightWall)
          case None =>
            left.summaryBetween(None, leftRightWall)
          case _ =>
            None
        }

        val rightLeftWall = low filter (_ > key)
        val rightBlock = high match {
          case Some(highVal) if highVal > key =>
            right.summaryBetween(rightLeftWall, high)
          case None =>
            right.summaryBetween(rightLeftWall, high)
          case _ =>
            None
        }

        val centreBlock = if (low.exists(_ > key) || high.exists(_ < key)) {// if low > key or high < key, don't include values
          None
        } else {
          mapReduce(values)
        }
        rereduce(Traversable.concat(leftBlock, centreBlock, rightBlock))
      }
    }

    def minKey = {
      left.minKey orElse Some(key)
    }

    def maxKey = {
      right.maxKey orElse Some(key)
    }

    def doBetween[U](low: Option[K], high: Option[K], f: ((K, V)) => U) = {
      val leftRightWall = high filter (_ < key)
      low match {
        case Some(lowVal) if lowVal < key =>
          left.doBetween(low, leftRightWall, f)
        case None =>
          left.doBetween(None, leftRightWall, f)
        case _ =>
          ()
      }

      if (low.forall(_ <= key) && high.forall(key <= _)) {// if low > key or high < key, don't include values
        for (v <- values) f((key, v))
      }

      val rightLeftWall = low filter (_ > key)
      high match {
        case Some(highVal) if highVal > key =>
          right.doBetween(rightLeftWall, high, f)
        case None =>
          right.doBetween(rightLeftWall, high, f)
        case _ =>
          ()
      }
    }

    def +(e: (K, V)) = {
      val (newKey, value) = e
      ordering.compare(newKey, key) match {
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

    private def skew = left match {
      case BranchNode(lKey, lValues, a, b, lLevel) if lLevel == this.level =>
        val t = BranchNode(this.key, this.values, b, this.right, this.level)
        val l = BranchNode(lKey, lValues, a, t, lLevel)
        l
      case _ => this
    }

    private def split = right match {
      case BranchNode(rKey, rValues, b, x, rLevel) if x.level == this.level =>
        val t = BranchNode(this.key, this.values, this.left, b, this.level)
        val r = BranchNode(rKey, rValues, t, x, rLevel + 1)
        r
      case _ => this
    }

    private def balance = this.skew.split

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