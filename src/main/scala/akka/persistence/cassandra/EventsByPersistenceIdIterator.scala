package akka.persistence.cassandra

import java.lang.{Long => JLong}
import java.nio.ByteBuffer

import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.query.{CassandraReadJournalConfig, CassandraReadStatements}

import scala.collection.JavaConverters._

import akka.persistence.PersistentRepr
import akka.persistence.PersistentRepr._
import akka.serialization.Serialization
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.{PreparedStatement, ResultSet, Row, Session}

final class EventsByPersistenceIdIterator(
    partitionId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    targetPartitionSize: Int,
    max: Int)(
    preparedSelectMessages: PreparedStatement,
    preparedCheckInUse: PreparedStatement,
    session: Session,
    serialization: Serialization)
  extends Iterator[PersistentRepr] {

  val iterator = new MessageIterator(
    partitionId,
    fromSequenceNr,
    toSequenceNr,
    targetPartitionSize,
    max,
    extractor,
    default,
    sequenceNumber,
    select,
    inUse,
    highestDeletedSequenceNumber,
    sequenceNumberColumn)

  override def hasNext: Boolean = iterator.hasNext
  override def next(): PersistentRepr = iterator.next()

  private[this] def extractor(row: Row): PersistentRepr =
    persistentFromByteBuffer(serialization, row.getBytes("message"))

  private[this] def inUse(partitionKey: String, currentPnr: Long): Boolean = {
    val execute: ResultSet = session.execute(preparedCheckInUse.bind(partitionId, currentPnr: JLong))
    if (execute.isExhausted) false
    else execute.one().getBool("used")
  }

  private[this] def default: PersistentRepr = PersistentRepr(Undefined)

  private[this] def sequenceNumberColumn: String = "sequence_nr"

  private[this] def sequenceNumber(element: PersistentRepr): Long = element.sequenceNr

  private[this] def select(
      partitionKey: String,
      currentPnr: Long,
      fromSnr: Long,
      toSnr: Long): Iterator[Row] =
    session.execute(preparedSelectMessages.bind(
      partitionKey,
      currentPnr: JLong,
      fromSnr: JLong,
      toSnr: JLong)).iterator.asScala

  // TODO: Fix.
  private[this] def highestDeletedSequenceNumber(partitionKey: String): Long = 0l

  private[this] def persistentFromByteBuffer(
      serialization: Serialization,
      b: ByteBuffer): PersistentRepr =
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
}
