package uk.me.jamespic.dougng.model

object DatasetName {
  private val Pattern = "dataset-(\\d+)-(\\d+)".r
  def apply(recordId: String) =  s"dataset-$recordId".replace("#", "").replace(":","-")
  def unapply(name: String) = name match {
    case Pattern(a, b) => Some(s"#$a:$b")
    case _ => None
  }
}