object BTreeWs {
  import com.sun.electric.database.geometry.btree._
  import unboxed._
  import unboxed.UnboxedImplicits._
  import java.lang.Math

  import java.lang.{Double => JDouble, Long => JLong}

  case class Summary(min: JDouble, max: JDouble, sum: JDouble, count: JLong);import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(362); 

  implicit val summaryBoxer = caseClassBoxer(Summary.apply _, Summary.unapply _);System.out.println("""summaryBoxer  : com.sun.electric.database.geometry.btree.unboxed.Unboxed[BTreeWs.Summary] = """ + $show(summaryBoxer ));$skip(62); 

  def summarise(k: JLong, v: JDouble) = Summary(v, v, v, 1L);System.out.println("""summarise: (k: Long, v: Double)BTreeWs.Summary""");$skip(151); 

  def combine(s1: Summary, s2: Summary) = {
    Summary(Math.min(s1.min, s2.min), Math.max(s1.max, s2.max), s1.sum + s2.sum, s1.count + s2.count)
  };System.out.println("""combine: (s1: BTreeWs.Summary, s2: BTreeWs.Summary)BTreeWs.Summary""");$skip(44); 

  val storage = new MemoryPageStorage(128);System.out.println("""storage  : com.sun.electric.database.geometry.btree.MemoryPageStorage = """ + $show(storage ));$skip(106); 

  val tree = new BTree[JLong, JDouble, Summary](storage, longBoxer, doubleBoxer, summarise _, combine _);System.out.println("""tree  : com.sun.electric.database.geometry.btree.BTree[Long,Double,BTreeWs.Summary] = """ + $show(tree ));$skip(56); 

  for (i <- 1L to 6L) {
    tree.insert(i, i.toDouble)
  };$skip(29); val res$0 = 
  tree.getValFromKey(1L);System.out.println("""res0: Double = """ + $show(res$0));$skip(34); val res$1 = 
  tree.getSummaryFromKeys(1L, 4L);System.out.println("""res1: BTreeWs.Summary = """ + $show(res$1))}
}
