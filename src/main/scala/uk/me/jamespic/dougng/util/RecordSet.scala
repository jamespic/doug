package uk.me.jamespic.dougng.util

trait RecordSet[A, I] extends Hibernatable {
  type ExtentType <: Extent
  type RecordType <: Record

  def apply(): ExtentType
  def apply(index: I): RecordType

  trait Extent {
    def apply(a: A): RecordType
  }

  trait Record {
    def index: I
    def apply(): A
    def update(a: A): Unit
    def save: Unit
  }
}