package uk.me.jamespic.dougng

package object util {
  def time[A](f: => A) = {
    val start = System.nanoTime
    val ret = f
    val end = System.nanoTime
    println(s"Ran in ${(end - start) / 1000000000.0}s")
    ret
  }

  def isPowOf2(l: Long) = {
    (l & (l - 1)) == 0
  }

  def pow2(i: Int) = 1L << i
  private val b = Array(0x2L, 0xCL, 0xF0L, 0xFF00L, 0xFFFF0000L, 0xFFFFFFFF00000000L)
  private val s = Array(1, 2, 4, 8, 16, 32)
  def log2(l: Long) = {
    var v = l
    var r = 0
    var i = 5
    while (i >= 0) {
      if ((v & b(i)) != 0L) {
        val si = s(i)
        v >>= si
        r |= si
      }
      i -= 1
    }
    r
  }

  implicit class LongOps(val l: Long) extends AnyVal {
    def >>|(i: Int) = if (i >= 64) (l >> 32 >> 32) else l >> i
  }
}