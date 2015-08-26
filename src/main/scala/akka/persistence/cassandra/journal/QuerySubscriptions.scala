package akka.persistence.cassandra.journal

import scala.collection.mutable

import akka.actor.{NoSerializationVerificationNeeded, DeadLetterSuppression, Actor, Terminated,
ActorRef}
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.QuerySubscriptions.{SubscribeAllPersistenceIds,
SubscriptionCommand, SubscribePersistenceId}

object QuerySubscriptions {

  sealed trait SubscriptionCommand

  /**
   * Subscribe the `sender` to changes (appended events) for a specific `persistenceId`.
   * Used by query-side. The journal will send [[EventAppended]] messages to
   * the subscriber when `asyncWriteMessages` has been called.
   */
  case class SubscribePersistenceId(persistenceId: String) extends SubscriptionCommand
  sealed trait SubscribePersistenceIdEvent
  case class EventAppended(persistenceId: String) extends DeadLetterSuppression with SubscribePersistenceIdEvent

  /**
   * Subscribe the `sender` to changes to all `persistenceId`s.
   * Used by query-side. The journal will send one [[CurrentPersistenceIds]] to the
   * subscriber followed by [[PersistenceIdAdded]] messages when new persistenceIds
   * are created.
   */
  case object SubscribeAllPersistenceIds extends SubscriptionCommand
  sealed trait SubscribeAllPersistenceIdsEvent
  case class CurrentPersistenceIds(allPersistenceIds: Set[String]) extends DeadLetterSuppression with SubscribeAllPersistenceIdsEvent
  case class PersistenceIdAdded(persistenceId: String) extends DeadLetterSuppression with SubscribeAllPersistenceIdsEvent

  /**
   * Subscribe the `sender` to changes (appended events) for a specific `tag`.
   * Used by query-side. The journal will send [[TaggedEventAppended]] messages to
   * the subscriber when `asyncWriteMessages` has been called.
   * Events are tagged by wrapping in [[akka.persistence.journal.leveldb.Tagged]]
   * via an [[akka.persistence.journal.EventAdapter]].
   */
  final case class SubscribeTag(tag: String) extends SubscriptionCommand
  sealed trait SubscribeTagEvent
  final case class TaggedEventAppended(tag: String) extends DeadLetterSuppression with SubscribeTagEvent

  final case class ReplayTaggedMessages(fromSequenceNr: Long, toSequenceNr: Long, max: Long,
    tag: String, replyTo: ActorRef) extends SubscriptionCommand with SubscribeTagEvent
  final case class ReplayedTaggedMessage(persistent: PersistentRepr, tag: String, offset: Long)
    extends DeadLetterSuppression with NoSerializationVerificationNeeded with SubscribeTagEvent
}

trait QuerySubscriptions { _: Actor =>

  // TODO: ???
  private val tagPersistenceIdPrefix = "$$$"

  private val persistenceIdSubscribers = new mutable.HashMap[String, mutable.Set[ActorRef]] with mutable.MultiMap[String, ActorRef]
  private val tagSubscribers = new mutable.HashMap[String, mutable.Set[ActorRef]] with mutable.MultiMap[String, ActorRef]
  private var allPersistenceIdsSubscribers = Set.empty[ActorRef]

  protected def handleQuerySubscriptions: Receive = {
    case subscription: SubscriptionCommand => subscription match {

      case SubscribePersistenceId(persistenceId: String) ⇒
        addPersistenceIdSubscriber(sender(), persistenceId)
        context.watch(sender())

      case SubscribeAllPersistenceIds ⇒
        addAllPersistenceIdsSubscriber(sender())
        context.watch(sender())
    }

    case Terminated(ref) ⇒
      removeSubscriber(ref)
  }

  protected def hasPersistenceIdSubscribers: Boolean = persistenceIdSubscribers.nonEmpty

  protected def addPersistenceIdSubscriber(subscriber: ActorRef, persistenceId: String): Unit =
    persistenceIdSubscribers.addBinding(persistenceId, subscriber)

  protected def removeSubscriber(subscriber: ActorRef): Unit = {
    val keys = persistenceIdSubscribers.collect { case (k, s) if s.contains(subscriber) ⇒ k }
    keys.foreach { key ⇒ persistenceIdSubscribers.removeBinding(key, subscriber) }

    val tagKeys = tagSubscribers.collect { case (k, s) if s.contains(subscriber) ⇒ k }
    tagKeys.foreach { key ⇒ tagSubscribers.removeBinding(key, subscriber) }

    allPersistenceIdsSubscribers -= subscriber
  }

  protected def hasTagSubscribers: Boolean = tagSubscribers.nonEmpty

  protected def addTagSubscriber(subscriber: ActorRef, tag: String): Unit =
    tagSubscribers.addBinding(tag, subscriber)

  protected def hasAllPersistenceIdsSubscribers: Boolean = allPersistenceIdsSubscribers.nonEmpty

  protected def addAllPersistenceIdsSubscriber(subscriber: ActorRef): Unit = {
    allPersistenceIdsSubscribers += subscriber
    subscriber ! QuerySubscriptions.CurrentPersistenceIds(allPersistenceIds)
  }

  private def notifyPersistenceIdChange(persistenceId: String): Unit =
    if (persistenceIdSubscribers.contains(persistenceId)) {
      val changed = QuerySubscriptions.EventAppended(persistenceId)
      persistenceIdSubscribers(persistenceId).foreach(_ ! changed)
    }

  private def notifyTagChange(tag: String): Unit =
    if (tagSubscribers.contains(tag)) {
      val changed = QuerySubscriptions.TaggedEventAppended(tag)
      tagSubscribers(tag).foreach(_ ! changed)
    }

  override protected def newPersistenceIdAdded(id: String): Unit = {
    if (hasAllPersistenceIdsSubscribers && !id.startsWith(tagPersistenceIdPrefix)) {
      val added = QuerySubscriptions.PersistenceIdAdded(id)
      allPersistenceIdsSubscribers.foreach(_ ! added)
    }
  }
}
