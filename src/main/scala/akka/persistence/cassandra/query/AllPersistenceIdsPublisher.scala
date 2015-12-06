package akka.persistence.cassandra.query

import scala.concurrent.duration.FiniteDuration

import akka.actor.Props
import com.datastax.driver.core.{Session, PreparedStatement}

import akka.persistence.cassandra.query.AllPersistenceIdsPublisher._

private[query] object AllPersistenceIdsPublisher {

  private[query] final case class AllPersistenceIdsSession(
    selectDistinctPersistenceIds: PreparedStatement,
    session: Session)

  def props(
      refreshInterval: Option[FiniteDuration], session: AllPersistenceIdsSession,
      config: CassandraReadJournalConfig): Props =
    Props(new AllPersistenceIdsPublisher(refreshInterval, session, config))
}

private[query] class AllPersistenceIdsPublisher(
    refreshInterval: Option[FiniteDuration], session: AllPersistenceIdsSession,
    config: CassandraReadJournalConfig)
  extends QueryActorPublisher[String, Set[String], String](refreshInterval, config.maxBufferSize) {
  
  override protected def query(state: Set[String], max: Long): Props = ???
  override protected def completionCondition(state: Set[String]): Boolean = ???
  override protected def initialState: Set[String] = ???
  override protected def updateBuffer(
    buf: Vector[String],
    newBuf: String,
    state: Set[String]): (Vector[String], Set[String]) = ???
}
