package akka.persistence.cassandra.query.journal

import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.persistence.Persistence
import akka.persistence.cassandra.journal.QuerySubscriptions.SubscribeAllPersistenceIdsEvent
import akka.stream.actor.{ActorPublisherMessage, ActorPublisher}
import akka.stream.actor.ActorPublisherMessage.{SubscriptionTimeoutExceeded, Cancel, Request}

/**
 * INTERNAL API
 */
private[journal] object AllPersistenceIdsPublisher {
  def props(liveQuery: Boolean, maxBufSize: Int, writeJournalPluginId: String): Props =
    Props(new AllPersistenceIdsPublisher(liveQuery, maxBufSize, writeJournalPluginId))

  private case object Continue
}

/**
 * INTERNAL API
 */
private[journal] class AllPersistenceIdsPublisher(liveQuery: Boolean, maxBufSize: Int, writeJournalPluginId: String)
  extends ActorPublisher[String] with DeliveryBuffer[String] with ActorLogging {

  val journal: ActorRef = Persistence(context.system).journalFor(writeJournalPluginId)

  def receive = init

  import akka.persistence.cassandra.journal.CassandraJournal
  def init: Receive = {
    case m: ActorPublisherMessage ⇒ m match {

      case _: Request ⇒
        journal ! QuerySubscriptions.SubscribeAllPersistenceIds
        context.become(active)

      case Cancel ⇒ context.stop(self)

      case SubscriptionTimeoutExceeded ⇒ context.stop(self)
    }
  }

  def active: Receive = {
    case e: SubscribeAllPersistenceIdsEvent ⇒ e match {

      case QuerySubscriptions.CurrentPersistenceIds(allPersistenceIds) ⇒
        buf ++= allPersistenceIds
        deliverBuf()
        if (streamComplete())
          onCompleteThenStop()

      case QuerySubscriptions.PersistenceIdAdded(persistenceId) ⇒
        if (liveQuery) {
          buf :+= persistenceId
          deliverBuf()
        }
    }

    case m: ActorPublisherMessage ⇒ m match {

      case _: Request ⇒
        deliverBuf()
        if (streamComplete())
          onCompleteThenStop()

      case Cancel ⇒ context.stop(self)
    }
  }

  private def streamComplete() = !liveQuery && buf.isEmpty
}
