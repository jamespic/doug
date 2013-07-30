object HeapTest {
  import scalaz._
  import java.util.{Random, PriorityQueue => JPQ}
  import java.lang.Integer
  import scala.collection.JavaConversions._
  import scala.collection.mutable.{PriorityQueue => SPQ}

  implicit object SeqFoldable extends Foldable[Seq] {
    def foldMap[A,B](fa: Seq[A])(f: A => B)(implicit F: Monoid[B]): B = {
      (F.zero /: fa){(x, y) => F.append(x, f(y))}
    }
    def foldRight[A, B](fa: Seq[A], z: => B)(f: (A, => B) => B): B = {
      (fa :\ z){(a, b) => f(a, b)}
    }
  };import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(538); 
  val rand = new Random;System.out.println("""rand  : java.util.Random = """ + $show(rand ));$skip(641); 
  def test[A](start: => A, insert: (A, Seq[Int]) => A, remove: A => A) = {
    var instance = start
    val sTime = System.nanoTime

    // populate
    instance = insert(instance, for (i <- 1 to 64) yield rand.nextInt)

    // run 1000 populate/remove iterations
    for (i <- 1 to 1000000) {
      for (j <- 1 to 8) {
        instance = remove(instance)
      }
      val newRows = for (i <- 1 to 8) yield rand.nextInt
      instance = insert(instance, newRows)
    }

    // clean up

    for (i <- 1 to 64) {
      instance = remove(instance)
    }

    val eTime = System.nanoTime
    println(s"Run time: ${(eTime - sTime) / 1e9}")
  };System.out.println("""test: [A](start: => A, insert: (A, Seq[Int]) => A, remove: A => A)Unit""");$skip(108); 

  test[Heap[Int]](Heap.Empty[Int],{(heap, i) => heap union Heap.fromData(i)},{heap => heap.uncons.get._2});$skip(108); 
  test[JPQ[Integer]](new JPQ[Integer],{(q, i) => q addAll i.map(Integer.valueOf); q}, {q => q.remove(); q});$skip(77); 
  test[SPQ[Int]](SPQ.empty[Int], {(q, i) => q ++= i}, {q => q.dequeue(); q});$skip(107); 
  test[Heap[Int]](Heap.Empty[Int],{(heap, i) => heap union Heap.fromData(i)},{heap => heap.uncons.get._2});$skip(108); 
  test[JPQ[Integer]](new JPQ[Integer],{(q, i) => q addAll i.map(Integer.valueOf); q}, {q => q.remove(); q});$skip(77); 
  test[SPQ[Int]](SPQ.empty[Int], {(q, i) => q ++= i}, {q => q.dequeue(); q})}
}
