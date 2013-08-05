package uk.me.jamespic.dougng.model.datamanager

import com.orientechnologies.orient.core.id.ORID
import uk.me.jamespic.dougng.model.Dataset

case class UpdateDatasets(tableName: String, datasets: Seq[Dataset])
case class DatasetsUpdated(datasets: Map[String, Nothing]) // FIXME: Nothing is the wrong type
