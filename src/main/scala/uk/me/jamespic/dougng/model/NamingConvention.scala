package uk.me.jamespic.dougng.model

object DatasetName extends NamingConvention("dataset")
object TimeGraphViewModelName extends NamingConvention("time-graph-view-model")

class NamingConvention(basename: String) {
  private val Pattern = s"$basename-(\\d+)-(\\d+)".r
  def apply(recordId: String) =  s"$basename-${recordId.replace("#", "").replace(":","-")}"
  def unapply(name: String) = name match {
    case Pattern(a, b) => Some(s"#$a:$b")
    case _ => None
  }
}