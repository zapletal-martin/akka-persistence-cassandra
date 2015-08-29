/*
package akka.persistence.cassandra.journal

import scala.concurrent.duration.FiniteDuration

import akka.actor._
import akka.persistence.Persistence
import akka.persistence.cassandra.journal.AllPersistenceIdsSubscriptions._
import akka.persistence.cassandra.query.journal.AllPersistenceIdsStore

object AllPersistenceIdsSubscriptions {
  case object SubscribeAllPersistenceIds
  case object GetCurrentPersistenceIds
  case class CurrentPersistenceIds(allPersistenceIds: Set[String]) extends DeadLetterSuppression

  case class Update()
}

class AllPersistenceIdsSubscriptions(writeJournalPluginId: String, refreshInterval: FiniteDuration)
  extends AllPersistenceIdsStore
  with Actor {

  val journal: ActorRef = Persistence(context.system).journalFor(writeJournalPluginId)

  private var diffSubscribers = Set.empty[ActorRef]
  private var newSubscribers = Set.empty[ActorRef]

  val tickTask =
    context.system.scheduler.schedule(refreshInterval, refreshInterval, self, Update)(context.dispatcher)

  protected def receive: Receive = {
    case SubscribeAllPersistenceIds ⇒
      addAllPersistenceIdsSubscriber(sender())
      context.watch(sender())

    case Terminated(ref) ⇒
      removeSubscriber(ref)

    case Update() ⇒
      journal ! GetCurrentPersistenceIds

    case CurrentPersistenceIds(current) =>
      updateSubscribers(current)
  }

  private def updateSubscribers(current: Set[String]): Unit = {
    newSubscribers.foreach(_ ! current)
    val diff = diff(current)
    diffSubscribers.foreach(_ ! diff)
    diffSubscribers ++= newSubscribers
    newSubscribers = Set.empty[ActorRef]
  }

  private def removeSubscriber(subscriber: ActorRef): Unit = {
    newSubscribers -= subscriber
    diffSubscribers -= subscriber
  }

  private def addAllPersistenceIdsSubscriber(subscriber: ActorRef): Unit = {
    newSubscribers += subscriber
    self ! Update
  }
}
*/
