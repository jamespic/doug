import uk.me.jamespic.dougng.util._
object FanoutTuning {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(114); 
  val values = (1 to 1000000) map (i => (i, i.toDouble));System.out.println("""values  : scala.collection.immutable.IndexedSeq[(Int, Double)] = """ + $show(values ));$skip(508); 
  for (i <- 3 to 6) {
    System.gc()
    System.gc()
    val fanout = 1 << i
    val startTime = System.currentTimeMillis
    val mrqs = new MapReduceQuickSort[Int, Double, Double](values, identity, _ + _, fanout)
    val buildTime = System.currentTimeMillis
    for (i <- 0 to 99) {
      mrqs.summaryBetween(10000 * i + 1, 10000 * i + 10000)
    }
    val endTime = System.currentTimeMillis
    println(s"With fanout $fanout, built in ${buildTime - startTime}ms, queried in ${endTime - buildTime}ms")
  }}
}
