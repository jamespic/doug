package uk.me.jamespic.dougng.model

import java.util.{List => JList, Map => JMap, ArrayList, HashMap}
import java.awt.Color

abstract class RowGraph extends Graph {
  var datasets: JList[Dataset] = new ArrayList()
  var hiddenRows: JList[String] = new ArrayList()
  var rowColours: JMap[String, Color] = new HashMap()
}