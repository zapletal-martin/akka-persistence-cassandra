package akka.persistence.cassandra.query

import java.lang.{Long => JLong}
import java.nio.ByteBuffer

import scala.annotation.tailrec
import scala.concurrent.{Promise, Future}

import akka.actor._
import akka.persistence.PersistentRepr
import akka.pattern.pipe
import akka.persistence.cassandra.query.QueryActorPublisher.ReplayDone
import akka.serialization.{SerializationExtension, Serialization}
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.{Row, PreparedStatement, Session, ResultSet}

import akka.persistence.cassandra.listenableFutureToFuture

private[query] object EventsByPersistenceIdFetcherActor {
  def props(
      persistenceId: String, from: Long, to: Long, maxBufferSize: Int, targetPartitionSize: Int,
      replyTo: ActorRef, session: Session, preparedSelectMessages: PreparedStatement,
      preparedCheckInUse: PreparedStatement, preparedSelectDeletedTo: PreparedStatement,
      settings: CassandraReadJournalConfig): Props =
    Props(
      new EventsByPersistenceIdFetcherActor(
        persistenceId, from, to, maxBufferSize, targetPartitionSize, replyTo, session,
        preparedSelectMessages, preparedCheckInUse, preparedSelectDeletedTo, settings))
      .withDispatcher(settings.pluginDispatcher)
}

/**
  * Non blocking reader of events by persistence id.
  * Iterates over messages, crossing partition boundaries.
  */
private[query] class EventsByPersistenceIdFetcherActor(
    persistenceId: String, from: Long, to: Long, maxBufferSize: Int, targetPartitionSize: Int,
    replyTo: ActorRef, session: Session, preparedSelectMessages: PreparedStatement,
    preparedCheckInUse: PreparedStatement, preparedSelectDeletedTo: PreparedStatement,
    settings: CassandraReadJournalConfig) extends Actor {

  private[this] sealed trait Action
  private[this] final case class StreamResultSet(
      partitionNr: Long, from: Long, rs: ResultSet) extends Action
  private[this] case object Finished extends Action with DeadLetterSuppression

  import context.dispatcher

  private[this] val serialization = SerializationExtension(context.system)

  override def preStart(): Unit = {
    highestDeletedSequenceNumber(persistenceId, preparedSelectDeletedTo).flatMap{ f =>
      val currentPnr = partitionNr(math.max(f + 1, from), targetPartitionSize)
      query(currentPnr, from, to)
    }.pipeTo(self)
  }

  override def receive: Receive = {
    case a: Action => a match {
      case StreamResultSet(pnr, f, rs) =>
        continue(pnr, f, rs).pipeTo(self)
      case Finished =>
        replyTo ! ReplayDone
        context.stop(self)
    }

    case Status.Failure(e) => throw e
  }

  private[this] def query(partitionNr: Long, from: Long, to: Long): Future[StreamResultSet] = {
    val boundStatement =
      preparedSelectMessages.bind(persistenceId, partitionNr: JLong, from: JLong, to: JLong)
    boundStatement.setFetchSize(maxBufferSize)

    listenableFutureToFuture(session.executeAsync(boundStatement))
      .map(StreamResultSet(partitionNr + 1, from, _))
  }

  private[this] def continue(partitionNr: Long, from: Long, resultSet: ResultSet): Future[Action] = {
    if(resultSet.isExhausted) {
      inUse(persistenceId, partitionNr).flatMap { i =>
        if (i && from < to) query(partitionNr, from, to)
        else Promise.successful(Finished).future
      }
    } else {
      val (fr, rs) = exhaustFetch(resultSet, from, resultSet.getAvailableWithoutFetching)
      listenableFutureToFuture(rs.fetchMoreResults()).map(StreamResultSet(partitionNr, fr, _))
    }
  }

  @tailrec private def exhaustFetch(resultSet: ResultSet, from: Long, n: Int): (Long, ResultSet) = {
    if(n == 0) {
      (from, resultSet)
    } else {
      val row = resultSet.one()
      replyTo ! extractor(row)
      exhaustFetch(resultSet, from + 1, n - 1)
    }
  }

  private[this] def highestDeletedSequenceNumber(
      partitionKey: String,
      preparedSelectDeletedTo: PreparedStatement): Future[Long] = {
    listenableFutureToFuture(session.executeAsync(preparedSelectDeletedTo.bind(partitionKey)))
      .map(r => Option(r.one()).map(_.getLong("deleted_to")).getOrElse(0))
  }

  private[this] def inUse(persistenceId: String, currentPnr: Long): Future[Boolean] = {
    session
      .executeAsync(preparedCheckInUse.bind(persistenceId, currentPnr: JLong))
      .map(rs => if (rs.isExhausted) false else rs.one().getBool("used"))
  }

  private[this] def partitionNr(sequenceNr: Long, targetPartitionSize: Int): Long =
    (sequenceNr - 1L) / targetPartitionSize

  private[this] def extractor(row: Row): PersistentRepr =
    persistentFromByteBuffer(serialization, row.getBytes("message"))

  private[this] def persistentFromByteBuffer(
      serialization: Serialization,
      b: ByteBuffer): PersistentRepr =
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
}
