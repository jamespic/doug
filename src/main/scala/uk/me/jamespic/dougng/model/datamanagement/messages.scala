package uk.me.jamespic.dougng.model.datamanagement
import com.orientechnologies.orient.core.record.impl.ODocument
import uk.me.jamespic.dougng.util.DetailedStats
import akka.actor.{Props, ActorRef}

/*
 * Message to create a data dependent actor. This is an actor that requires a
 * ReplacablePool, and that may require permission to update itself at various points.
 * They can request permission to update from their parent, with RequestPermissionToUpdate,
 * and once they receive permission (via a PermissionToUpdate), they have permission until
 * they relinquish it with an UpdateComplete.
 */
case class CreateDataDependentActor(constructor: ReplacablePool => Props, name: String)
case class ActorCreated(actor: ActorRef, name: String)
case class GetDataset(recordId: String)

/*
 * Message informing a parent that a DatasetActor thinks it needs to update itself,
 * probably because it was restarted
 */
case object RequestPermissionToUpdate
case object RequestExclusiveDatabaseAccess

/*
 * Messages from Documents to DatasetActors, informing them of data changes, and allowing
 * them to act on them
 */
sealed trait PleaseUpdate
case class DocumentsInserted(doc: Traversable[ODocument]) extends PleaseUpdate
case class DatasetUpdate(idsAffected: Set[String]) extends PleaseUpdate
case class PresentationUpdate(idsAffected: Set[String]) extends PleaseUpdate
case object PleaseUpdate extends PleaseUpdate

/*
 * Messages from DatasetActors to Documents, either acknowledging requests,
 * or requesting to be notified when all changes are complete.
 */
sealed trait UpdateComplete
case object AllDone extends UpdateComplete
case object RemindMeLater extends UpdateComplete

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
case object GetLastError
case class LastError(ex: Option[Throwable])
