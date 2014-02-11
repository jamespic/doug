package uk.me.jamespic.dougng.model
import javax.persistence.{Id, Version}
import java.util.{List => JList, Map => JMap, ArrayList, HashMap}
import java.awt.Color

class TimeGraph extends RowGraph {
  var granularity: Int = 20000
  var maxDatasets: JList[Dataset] = new ArrayList()
}