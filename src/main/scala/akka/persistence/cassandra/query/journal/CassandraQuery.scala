package akka.persistence.cassandra.query.journal

import scala.concurrent.Future

import akka.actor.Actor
import akka.persistence.Persistence
import akka.persistence.cassandra.journal.CassandraJournal.{CurrentAllPersistenceIds, GetCurrentAllPersistenceIds}
import akka.pattern.ask
import akka.util.Timeout

//TODO: Replace with actual implementation
trait CassandraQuery { _: Actor =>

  protected def writeJournalPluginId: String
  protected implicit def databaseQueryTimeout: Timeout

  private val journal = Persistence(context.system).journalFor(writeJournalPluginId)

  def curentAllPersistenceIds(): Future[Set[String]] = {
    implicit val ec = context.dispatcher
    (journal ? GetCurrentAllPersistenceIds).mapTo[CurrentAllPersistenceIds].map(_.allPersistenceIds)
  }
}