package akka.persistence.cassandra.query

import scala.concurrent.duration.FiniteDuration

import akka.actor.Props
import akka.persistence.PersistentRepr
import akka.persistence.query.EventEnvelope
import com.datastax.driver.core.{PreparedStatement, Session}

private[query] object EventsByPersistenceIdPublisher {
  def props(
      persistenceId: String, fromSeqNr: Long, toSeqNr: Long, refreshInterval: Option[FiniteDuration],
      maxBufSize: Int, targetPartitionSize: Int, selectEventsByPersistenceId: PreparedStatement,
      selectInUse: PreparedStatement, selectDeletedTo: PreparedStatement, session: Session,
      config: CassandraReadJournalConfig): Props =
    Props(
      new EventsByPersistenceIdPublisher(persistenceId, fromSeqNr, toSeqNr, refreshInterval,
        maxBufSize, targetPartitionSize, selectEventsByPersistenceId, selectInUse, selectDeletedTo,
        session, config))
}

private[query] class EventsByPersistenceIdPublisher(
    persistenceId: String, fromSeqNr: Long, toSeqNr: Long, refreshInterval: Option[FiniteDuration],
    maxBufSize: Int, targetPartitionSize: Int, selectEventsByPersistenceId: PreparedStatement,
    selectInUse: PreparedStatement, selectDeletedTo: PreparedStatement, session: Session,
    config: CassandraReadJournalConfig)
  extends QueryActorPublisher[EventEnvelope, Long, PersistentRepr](refreshInterval, maxBufSize) {

  private[this] val step = maxBufSize

  override protected def query(state: Long, max: Long): Props = {
    val to = Math.min(Math.min(state + step, toSeqNr), state + max)

    EventsByPersistenceIdFetcherActor.props(
      persistenceId, state, to, max.toInt, targetPartitionSize, self, session,
      selectEventsByPersistenceId, selectInUse, selectDeletedTo, config)
  }

  override protected def initialState: Long = Math.max(1, fromSeqNr)

  override def updateBuffer(
      buffer: Vector[EventEnvelope],
      newBuffer: PersistentRepr,
      state: Long): (Vector[EventEnvelope], Long) = {

    val addToBuffer = toEventEnvelope(newBuffer, state - 1)
    val newState = newBuffer.sequenceNr + 1

    (buffer :+ addToBuffer, newState)
  }

  override protected def completionCondition(state: Long): Boolean = state > toSeqNr

  private[this] def toEventEnvelope(persistentRepr: PersistentRepr, offset: Long): EventEnvelope =
    EventEnvelope(offset, persistentRepr.persistenceId, persistentRepr.sequenceNr, persistentRepr.payload)
}
