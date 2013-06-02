package uk.me.jamespic.dougng.util

trait RecordSet[A] extends Hibernatable {
  type IndexType
  type ExtentType <: Extent
  type RecordType <: Record

  def apply(): ExtentType
  def apply(index: IndexType): RecordType

  trait Extent {
    def apply(a: A): Record
  }

  trait Record {
      def index: IndexType
      def apply(): A
      def update(a: A): Unit
      def save: Unit
    }
}