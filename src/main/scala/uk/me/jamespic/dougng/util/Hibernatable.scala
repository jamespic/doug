package uk.me.jamespic.dougng.util

trait Hibernatable {
  def hibernate(): Unit
  def sync[B](f: => B) = this.synchronized(f)
}