package uk.me.jamespic.dougng.util

class TransactionContext[T <: Hibernatable](val trans: T) {// trans needs to be public, to use trans.type
  private var entryCount = 0

  def apply[A](f: trans.type => A): A = apply(f(trans))

  def apply[A](f: => A): A = trans.sync {
    entryCount += 1
    val ret = f
    entryCount -= 1
    if (entryCount == 0) trans.hibernate
    ret
  }

}