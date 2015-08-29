package akka.persistence.cassandra.query.journal

import scala.concurrent.duration._

import akka.actor.{DeadLetterSuppression, ActorLogging, ActorRef, Props}
import akka.persistence.Persistence
import akka.persistence.cassandra.journal.CassandraJournal.{CurrentAllPersistenceIds,
GetCurrentAllPersistenceIds}
import akka.persistence.cassandra.query.journal.AllPersistenceIdsPublisher.Update
import akka.stream.actor.ActorPublisherMessage.{Cancel, SubscriptionTimeoutExceeded, Request}
import akka.stream.actor.{ActorPublisherMessage, ActorPublisher}
import akka.util.Timeout

private[journal] object AllPersistenceIdsPublisher {
  def props(refreshInterval: Option[FiniteDuration], maxBufSize: Int, writeJournalPluginId: String): Props =
    Props(new AllPersistenceIdsPublisher(refreshInterval, maxBufSize, writeJournalPluginId))

  case object Update
}

private[journal] class AllPersistenceIdsPublisher(refreshInterval: Option[FiniteDuration], maxBufSize: Int, writeJournalPluginId: String)
  extends ActorPublisher[String]
  with DeliveryBuffer[String]
  with AllPersistenceIdsStore
  with ActorLogging {

  val journal: ActorRef = Persistence(context.system).journalFor(writeJournalPluginId)

  val timeout = Timeout(1.seconds)
  //TODO: FIX
  val tickTask =
    context.system.scheduler.schedule(timeout.duration, timeout.duration, self, Update)(context.dispatcher)

  def receive: Receive = {

    case CurrentAllPersistenceIds(current) ⇒
      val dif = diff(current)
      addPersistenceIds(current)
      buf ++= dif
      deliverBuf()
      if (streamComplete())
        onCompleteThenStop()

    case Update ⇒
      journal ! GetCurrentAllPersistenceIds

    case m: ActorPublisherMessage ⇒ m match {

      case _: Request ⇒
        deliverBuf()
        if (streamComplete())
          onCompleteThenStop()

      case Cancel ⇒ context.stop(self)

      case SubscriptionTimeoutExceeded ⇒ context.stop(self)
    }
  }

  override def preStart(): Unit = {
    journal ! GetCurrentAllPersistenceIds
  }

  private def streamComplete() = !refreshInterval.isDefined && buf.isEmpty
}
