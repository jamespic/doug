package uk.me.jamespic.dougng.model.datamanagement

import com.orientechnologies.orient.`object`.db.OObjectDatabasePool
import com.orientechnologies.orient.core.record.impl.ODocument
import uk.me.jamespic.dougng.util.Stats
import akka.actor.{Props, ActorRef}

/*
 * Message to create a data dependent actor. This is an actor that requires a
 * ReplacablePool, and that may require permission to update itself at various points.
 * They can request permission to update from their parent, with RequestPermissionToUpdate,
 * and once they receive permission (via a PermissionToUpdate), they have permission until
 * they relinquish it with an UpdateComplete.
 */
case class ConstructorInfo(pool: OObjectDatabasePool, database: ActorRef)
case class CreateDataDependentActor(constructor: ConstructorInfo => Props, name: String)
case class ActorCreated(actor: ActorRef, name: String)
case class GetDataset(recordId: String)

/*
 * Message informing a parent that a DatasetActor thinks it needs to update itself,
 * probably because it was restarted
 */
case object RequestPermissionToRead
case object RequestPermissionToUpdate
case object RequestExclusiveDatabaseAccess

/*
 * Messages from Database to Data Dependent Actors, informing them of data changes, and allowing
 * them to act on them.
 */
sealed trait PleaseUpdate
sealed trait ForwardableUpdateInfo extends PleaseUpdate
case class DocumentsInserted(doc: Traversable[ODocument]) extends ForwardableUpdateInfo
case class DocumentsDeleted(doc: Set[String]) extends ForwardableUpdateInfo
case class DatasetUpdate(idsAffected: Set[String]) extends ForwardableUpdateInfo
case class PresentationUpdate(idsAffected: Set[String]) extends ForwardableUpdateInfo
case object PleaseRead extends PleaseUpdate
case object PleaseUpdate extends PleaseUpdate
case object ExclusiveAccessGranted extends PleaseUpdate

/*
 * Messages from Data Dependent Actors to Database, either acknowledging requests,
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
case class GetMetadata(corrId: Any = null)
case class GetAllSummaries(ranges: Iterable[(Long, Long)], corrId: Any = null)
case class GetSummaries(rows: Iterable[String], ranges: Iterable[(Long, Long)], corrId: Any = null)
case class GetInRange(rows: Iterable[String], from: Long, to: Long, corrId: Any = null)
case class GetAllInRange(from: Long, to: Long, corrId: Any = null)
case class Metadata(min: Long, max: Long, rows: Set[String], corrId: Any = null)
case class Summaries(result: Map[String, Map[(Long, Long), Option[Stats]]], corrId: Any = null)
case class Ranges(result: Map[String, Iterable[(Long, Double)]], corrId: Any = null)
case class GetLastError(corrId: Any = null)
case class LastError(ex: Option[Throwable], corrId: Any = null)
