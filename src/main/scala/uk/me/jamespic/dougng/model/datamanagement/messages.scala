package uk.me.jamespic.dougng.model.datamanagement

import uk.me.jamespic.dougng.model.Dataset
import com.orientechnologies.orient.core.record.impl.ODocument
import uk.me.jamespic.dougng.util.DetailedStats
import akka.actor.Props

/*
 * Message to create a data dependent actor. This is an actor that requires a
 * ReplacablePool, and that may require permission to update itself at various points.
 * They can request permission to update from their parent, with RequestPermissionToUpdate,
 * and once they receive permission (via a PermissionToUpdate), they have permission until
 * they relinquish it with an UpdateComplete.
 */
case class CreateDataDependentActor(constructor: ReplacablePool => Props)

/*
 * Message informing a parent that a DatasetActor thinks it needs to update itself,
 * probably because it was restarted
 */
case object RequestPermissionToUpdate


/*
 * Messages from Documents to DatasetActors, informing them of data changes, and allowing
 * them to act on them
 */
sealed trait PermissionToUpdate
case class TablesUpdated(tables: Set[String]) extends PermissionToUpdate
case class DocumentInserted(doc: ODocument) extends PermissionToUpdate
case object PermissionToUpdate extends PermissionToUpdate

/*
 * Messages from DatasetActors to Documents, either acknowledging requests,
 * or requesting to be notified when all changes are complete.
 */
sealed trait UpdateComplete
case object AllDone extends UpdateComplete
case object RemindMeLater extends UpdateComplete

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
case class GetAllSummaries(ranges: Iterable[(Long, Long)])
case class GetSummaries(rows: Iterable[String], ranges: Iterable[(Long, Long)])
case class GetInRange(rows: Iterable[String], from: Long, to: Long)
case class GetAllInRange(from: Long, to: Long)
case class Metadata(min: Long, max: Long, rows: Set[String])
case class Summaries(result: Map[String, Map[(Long, Long), Option[DetailedStats]]])
case class Ranges(result: Map[String, Iterable[(Long, Double)]])

