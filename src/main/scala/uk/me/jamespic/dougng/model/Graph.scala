package uk.me.jamespic.dougng.model

import javax.persistence.{Id, Version}

abstract class Graph {
  var name: String = "Graph"
  @Id var id: String = _
  @Version var version: String = _

}