package uk.me.jamespic.dougng

package object util {
  def time[A](f: => A) = {
    val start = System.nanoTime
    val ret = f
    val end = System.nanoTime
    println(s"Ran in ${(end - start) / 1000000000.0}s")
    ret
  }
}