package akka.persistence.cassandra.query.scaladsl

import scala.concurrent.duration.FiniteDuration
import akka.actor.ExtendedActorSystem
import akka.persistence.query._
import akka.persistence.query.scaladsl._
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import akka.persistence.cassandra.query.CassandraReadJournalConfig
import akka.persistence.cassandra.query.CassandraReadStatements
import akka.persistence.cassandra.query.EventsByTagPublisher
import akka.util.ByteString
import java.net.URLEncoder
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.CassandraStatements
import com.datastax.driver.core.utils.UUIDs
import akka.persistence.cassandra.journal.CassandraJournal
import java.time.LocalDateTime
import java.util.UUID
import java.time.ZoneOffset
import java.time.LocalDate
import com.datastax.driver.core.PreparedStatement

object CassandraReadJournal {
  final val Identifier = "cassandra-query-journal"
}

// FIXME documentation (see LeveldbReadJournal)
class CassandraReadJournal(system: ExtendedActorSystem, config: Config)
  extends ReadJournal
  with EventsByTagQuery
  with CurrentEventsByTagQuery {

  // FIXME session should be lazy and creations should be done with retries
  val writePluginConfig = new CassandraJournalConfig(system.settings.config.getConfig(config.getString("write-plugin")))
  val queryPluginConfig = new CassandraReadJournalConfig(config, writePluginConfig)
  val cluster = writePluginConfig.clusterBuilder.build
  val session = cluster.connect()
  val writeStatements: CassandraStatements = new CassandraStatements {
    def config: CassandraJournalConfig = writePluginConfig
  }
  val queryStatements: CassandraReadStatements = new CassandraReadStatements {
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
  val preparedSelectEventsByTag: Vector[PreparedStatement] =
    (1 to writePluginConfig.maxTagId).map { tagId =>
      session.prepare(queryStatements.selectEventsByTag(tagId))
        .setConsistencyLevel(queryPluginConfig.readConsistency)
    }.toVector

  private val firstOffset: UUID = {
    val timestamp = LocalDate.parse(queryPluginConfig.firstTimeBucket, CassandraJournal.timeBucketFormatter)
      .atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli
    UUIDs.startOf(timestamp)
  }

  def offsetUuid(timestamp: Long): UUID =
    if (timestamp == 0L) firstOffset else UUIDs.startOf(timestamp)

  private def selectStatement(tag: String): PreparedStatement =
    preparedSelectEventsByTag(writePluginConfig.tags(tag) - 1)

  override def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = {
    import queryPluginConfig._
    Source.actorPublisher[EventEnvelope](EventsByTagPublisher.props(tag, offsetUuid(offset),
      Some(refreshInterval), queryPluginConfig, session, selectStatement(tag))).mapMaterializedValue(_ ⇒ ())
      .named("eventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))
  }

  override def currentEventsByTag(tag: String, offset: Long = 0L): Source[EventEnvelope, Unit] = {
    import queryPluginConfig._
    Source.actorPublisher[EventEnvelope](EventsByTagPublisher.props(tag, offsetUuid(offset),
      None, queryPluginConfig, session, selectStatement(tag))).mapMaterializedValue(_ ⇒ ())
      .named("currentEventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))
  }

  // FIXME we should also provide queries that takes a UUID as offset parameter and returns a
  //       Source of some envelope containing UUID offset values

}
