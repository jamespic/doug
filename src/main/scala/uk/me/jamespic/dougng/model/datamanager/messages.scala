package uk.me.jamespic.dougng.model.datamanager

import uk.me.jamespic.dougng.model.Dataset
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.record.impl.ODocument
import uk.me.jamespic.dougng.util.DetailedStats

/*
 * Messages from Documents to DatasetActors, informing them of data changes
 */
case class TablesUpdated(tables: Set[String], db: ODatabaseDocumentTx)
case class DocumentInserted(doc: ODocument)

/*
 * Messages from DatasetActors to Documents, either acknowledging requests,
 * or requesting to be notified when all changes are complete
 */
case object AllDone
case object RemindMeLater

/*
 * Messages between DatasetActors and Documents, discussing which tables
 * they're interested in. This helps avoid telling DatasetActors about
 * Documents they don't care about
 */
case object WhichTablesAreYouInterestedIn
case object AllOfThem
case class TheseTables(tables: Set[String])

/*
 * Messages between DatasetActors and other actors interested in their data
 */
case object ListenTo
case object UnlistenTo
case object DataUpdatedNotification
case object GetMetadata
case class GetAllSummaries(ranges: Seq[(Long, Long)])
case class GetSummaries(rows: Seq[String], ranges: Seq[(Long, Long)])
case class GetInRange(rows: Seq[String], from: Long, to: Long)
case class GetAllInRange(from: Long, to: Long)
case class Metadata(min: Long, max: Long, rows: Seq[String])
case class Summaries(result: Map[String, Map[(Long, Long), Option[DetailedStats]]])
case class Ranges(result: Map[String, Seq[(Long, Double)]])