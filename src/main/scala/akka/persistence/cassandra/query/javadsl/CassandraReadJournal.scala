package akka.persistence.cassandra.query.javadsl

import scala.concurrent.duration._

import akka.persistence.query.EventEnvelope
import akka.persistence.query.javadsl._
import akka.stream.javadsl.Source

/**
 * Java API: [[akka.persistence.query.javadsl.ReadJournal]] implementation for Cassandra.
 *
 * It is retrieved with:
 * {{{
 * CassandraReadJournal queries =
 *   PersistenceQuery.get(system).getReadJournalFor(CassandraReadJournal.class, CassandraReadJournal.Identifier());
 * }}}
 *
 * Corresponding Scala API is in [[akka.persistence.cassandra.query.scaladsl.CassandraReadJournal]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"cassandra-query-journal"`
 * for the default [[CassandraReadJournal#Identifier]]. See `reference.conf`.
 *
 */
class CassandraReadJournal(scaladslReadJournal: akka.persistence.cassandra.query.scaladsl.CassandraReadJournal)
  extends ReadJournal
  with EventsByTagQuery
  with CurrentEventsByTagQuery {

  override def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] =
    scaladslReadJournal.eventsByTag(tag, offset).asJava

  override def currentEventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] =
    scaladslReadJournal.currentEventsByTag(tag, offset).asJava
}

object CassandraReadJournal {
  /**
   * The default identifier for [[CassandraReadJournal]] to be used with
   * [[akka.persistence.query.PersistenceQuery#getReadJournalFor]].
   *
   * The value is `"cassandra-query-journal"` and corresponds
   * to the absolute path to the read journal configuration entry.
   */
  final val Identifier = akka.persistence.cassandra.query.scaladsl.CassandraReadJournal.Identifier
}

