package akka.persistence.cassandra.query

import scala.concurrent.duration._
import com.typesafe.config.Config
import com.datastax.driver.core.ConsistencyLevel
import akka.persistence.cassandra.journal.CassandraJournalConfig

class CassandraReadJournalConfig(config: Config, writePluginConfig: CassandraJournalConfig) {
  val refreshInterval: FiniteDuration = config.getDuration("refresh-interval", MILLISECONDS).millis
  val maxBufferSize: Int = config.getInt("max-buffer-size")
  val readConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read-consistency"))
  val firstTimeBucket: String = config.getString("first-time-bucket")
  val eventualConsistencyDelay: FiniteDuration =
    config.getDuration("eventual-consistency-delay", MILLISECONDS).millis

  val eventsByTagView: String = writePluginConfig.eventsByTagView
  val keyspace: String = writePluginConfig.keyspace

  // FIXME use-dispatcher setting
}
