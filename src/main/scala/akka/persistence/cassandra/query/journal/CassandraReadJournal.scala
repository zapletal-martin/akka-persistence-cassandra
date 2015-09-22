package akka.persistence.cassandra.query.journal

import scala.concurrent.duration.{FiniteDuration, _}

import akka.actor.ExtendedActorSystem
import akka.persistence.query._
import akka.persistence.query.scaladsl.ReadJournal
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Source
import com.typesafe.config.Config

object CassandraReadJournal {
  final val Identifier = "cassandra-query-journal"
}

class CassandraReadJournal(system: ExtendedActorSystem, config: Config) extends ReadJournal {

  private val serialization = SerializationExtension(system)
  private val defaulRefreshInterval: Option[FiniteDuration] = Some(config.getDuration("refresh-interval", MILLISECONDS).millis)
  private val writeJournalPluginId: String = config.getString("write-plugin")
  private val maxBufSize: Int = config.getInt("max-buffer-size")

  override def query[T, M](q: Query[T, M], hints: Hint*): Source[T, M] = q match {
    case EventsByPersistenceId(pid, from, to) ⇒ ??? //eventsByPersistenceId(pid, from, to, hints)
    case AllPersistenceIds                    ⇒ allPersistenceIds(hints)
    case EventsByTag(tag, offset)             ⇒ ??? //eventsByTag(tag, offset, hints)
    case unknown                              ⇒ unsupportedQueryType(unknown)
  }

  def allPersistenceIds(hints: Seq[Hint]): Source[String, Unit] = {
    Source.actorPublisher[String](AllPersistenceIdsPublisher.props(refreshInterval(hints), maxBufSize))
      .mapMaterializedValue(_ ⇒ ())
      .named("allPersistenceIds")
  }

  private def refreshInterval(hints: Seq[Hint]): Option[FiniteDuration] =
    if (hints.contains(NoRefresh))
      None
    else
      hints.collectFirst { case RefreshInterval(interval) ⇒ interval }.orElse(defaulRefreshInterval)

  private def unsupportedQueryType[M, T](unknown: Query[T, M]): Nothing =
    throw new IllegalArgumentException(s"${getClass.getSimpleName} does not implement the ${unknown.getClass.getName} query type!")
}