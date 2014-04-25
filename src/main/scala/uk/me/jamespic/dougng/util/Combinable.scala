package uk.me.jamespic.dougng.util

trait Combinable[D, T <: Combinable[D, T]] {this: T =>
  def +(d: D): T
  def ++(t: T): T
}

trait Start[D, T <: Combinable[D, T]] extends Combinable[D, T] {this: T =>
}