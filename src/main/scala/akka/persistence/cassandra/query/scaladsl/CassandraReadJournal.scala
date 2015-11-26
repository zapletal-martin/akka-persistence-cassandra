package akka.persistence.cassandra.query.scaladsl

import java.net.URLEncoder
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.UUID

import akka.actor.ExtendedActorSystem
import akka.persistence.cassandra.journal.CassandraJournal
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.CassandraStatements
import akka.persistence.cassandra.query.CassandraReadJournalConfig
import akka.persistence.cassandra.query.CassandraReadStatements
import akka.persistence.cassandra.query.EventsByTagPublisher
import akka.persistence.cassandra.query.UUIDEventEnvelope
import akka.persistence.query._
import akka.persistence.query.scaladsl._
import akka.stream.ActorAttributes
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.Config

object CassandraReadJournal {
  final val Identifier = "cassandra-query-journal"
}

// FIXME documentation (see LeveldbReadJournal)
class CassandraReadJournal(system: ExtendedActorSystem, config: Config)
  extends ReadJournal
  with EventsByTagQuery
  with CurrentEventsByTagQuery {

  // FIXME session should be lazy and creations should be done with retries
  private val writePluginConfig = new CassandraJournalConfig(system.settings.config.getConfig(config.getString("write-plugin")))
  private val queryPluginConfig = new CassandraReadJournalConfig(config, writePluginConfig)
  private val cluster = writePluginConfig.clusterBuilder.build
  private val session = cluster.connect()
  private val writeStatements: CassandraStatements = new CassandraStatements {
    def config: CassandraJournalConfig = writePluginConfig
  }
  private val queryStatements: CassandraReadStatements = new CassandraReadStatements {
    override def config = queryPluginConfig
  }
  if (writePluginConfig.keyspaceAutoCreate) {
    akka.persistence.cassandra.retry(writePluginConfig.keyspaceAutoCreateRetries) {
      session.execute(writeStatements.createKeyspace)
    }
  }
  session.execute(writeStatements.createTable)
  for (tagId <- 1 to writePluginConfig.maxTagId)
    session.execute(writeStatements.createEventsByTagMaterializedView(tagId))
  private val preparedSelectEventsByTag: Vector[PreparedStatement] =
    (1 to writePluginConfig.maxTagId).map { tagId =>
      session.prepare(queryStatements.selectEventsByTag(tagId))
        .setConsistencyLevel(queryPluginConfig.readConsistency)
    }.toVector

  private def selectStatement(tag: String): PreparedStatement =
    preparedSelectEventsByTag(writePluginConfig.tags(tag) - 1)

  val firstOffset: UUID = {
    val timestamp = LocalDate.parse(queryPluginConfig.firstTimeBucket, CassandraJournal.timeBucketFormatter)
      .atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli
    UUIDs.startOf(timestamp)
  }

  def offsetUuid(timestamp: Long): UUID =
    if (timestamp == 0L) firstOffset else UUIDs.startOf(timestamp)

  override def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = {
    eventsByTag(tag, offsetUuid(offset))
      .map(env => EventEnvelope(
        offset = UUIDs.unixTimestamp(env.offset),
        persistenceId = env.persistenceId,
        sequenceNr = env.sequenceNr,
        event = env.event))
  }

  // FIXME document that this is exclusive, while the timestamped is inclusive
  def eventsByTag(tag: String, offset: UUID): Source[UUIDEventEnvelope, Unit] = {
    import queryPluginConfig._
    Source.actorPublisher[UUIDEventEnvelope](EventsByTagPublisher.props(tag, offset,
      Some(refreshInterval), queryPluginConfig, session, selectStatement(tag)))
      .mapMaterializedValue(_ ⇒ ())
      .named("eventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))
      .withAttributes(ActorAttributes.dispatcher(pluginDispatcher))
  }

  override def currentEventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = {
    currentEventsByTag(tag, offsetUuid(offset))
      .map(env => EventEnvelope(
        offset = UUIDs.unixTimestamp(env.offset),
        persistenceId = env.persistenceId,
        sequenceNr = env.sequenceNr,
        event = env.event))
  }

  def currentEventsByTag(tag: String, offset: UUID): Source[UUIDEventEnvelope, Unit] = {
    import queryPluginConfig._
    Source.actorPublisher[UUIDEventEnvelope](EventsByTagPublisher.props(tag, offset,
      None, queryPluginConfig, session, selectStatement(tag)))
      .mapMaterializedValue(_ ⇒ ())
      .named("currentEventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))
      .withAttributes(ActorAttributes.dispatcher(pluginDispatcher))
  }

}
