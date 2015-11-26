package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.math.min
import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.journal.Tagged
import akka.serialization.SerializationExtension
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{ LoggingRetryPolicy, RetryPolicy }
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.Config
import scala.util.Try
import scala.util.Success
import scala.util.Failure

class CassandraJournal(cfg: Config) extends AsyncWriteJournal with CassandraRecovery with CassandraConfigChecker with CassandraStatements {
  import CassandraJournal._

  val config = new CassandraJournalConfig(cfg)
  val serialization = SerializationExtension(context.system)

  import config._

  val session = connect()

  case class MessageId(persistenceId: String, sequenceNr: Long)

  if (config.keyspaceAutoCreate) {
    retry(config.keyspaceAutoCreateRetries) {
      session.execute(createKeyspace)
    }
  }
  session.execute(createTable)
  session.execute(createMetatdataTable)
  session.execute(createConfigTable)
  session.execute(createEventsByTagMaterializedView(1))

  val persistentConfig: Map[String, String] = initializePersistentConfig

  val preparedWriteMessage = session.prepare(writeMessage)
  val preparedWriteMessageWithTag1 = session.prepare(writeMessageWithTag(1))
  val preparedDeletePermanent = session.prepare(deleteMessage)
  val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)
  val preparedCheckInUse = session.prepare(selectInUse).setConsistencyLevel(readConsistency)
  val preparedWriteInUse = session.prepare(writeInUse)
  val preparedSelectHighestSequenceNr = session.prepare(selectHighestSequenceNr).setConsistencyLevel(readConsistency)
  val preparedSelectDeletedTo = session.prepare(selectDeletedTo).setConsistencyLevel(readConsistency)
  val preparedInsertDeletedTo = session.prepare(insertDeletedTo).setConsistencyLevel(writeConsistency)

  private def connect(): Session = {
    retry(config.connectionRetries + 1, config.connectionRetryDelay.toMillis)(clusterBuilder.build().connect())
  }

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    // we need to preserve the order / size of this sequence even though we don't map
    // AtomicWrites 1:1 with a C* insert
    val serialized = messages.map(aw => Try {
      SerializedAtomicWrite(
        aw.payload.head.persistenceId,
        aw.payload.map { pr =>
          val (pr2, tags) = pr.payload match {
            case Tagged(payload, tags) ⇒
              (pr.withPayload(payload), tags)
            case _ ⇒ (pr, Set.empty[String])
          }
          //FIXME handle more than one tag
          Serialized(pr.sequenceNr, persistentToByteBuffer(pr2), tags)
        })
    })
    val result = serialized.map(a => a.map(_ => ()))

    val byPersistenceId = serialized.collect { case Success(caw) => caw }.groupBy(_.persistenceId).values
    val boundStatements = byPersistenceId.map(statementGroup)

    val batchStatements = boundStatements.map { unit =>
      unit.length match {
        case 0 => Future.successful(())
        case 1 => execute(unit.head)
        case _ => executeBatch(batch => unit.foreach(batch.add))
      }
    }
    val promise = Promise[Seq[Try[Unit]]]()

    Future.sequence(batchStatements).onComplete {
      case Success(_) => promise.complete(Success(result))
      case Failure(e) => promise.failure(e)
    }

    promise.future
  }

  private def statementGroup(atomicWrites: Seq[SerializedAtomicWrite]): Seq[BoundStatement] = {
    val maxPnr = partitionNr(atomicWrites.last.payload.last.sequenceNr)
    val firstSeq = atomicWrites.head.payload.head.sequenceNr
    val minPnr = partitionNr(firstSeq)
    val persistenceId: String = atomicWrites.head.persistenceId
    val all = atomicWrites.flatMap(_.payload)

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    require(maxPnr - minPnr <= 1, "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[BoundStatement] = all.map { m =>
      // use same clock source as the UUID for the timeBucket
      val nowUuid = UUIDs.timeBased()
      val now = UUIDs.unixTimestamp(nowUuid)
      if (m.tags.isEmpty)
        preparedWriteMessage.bind(persistenceId, maxPnr: JLong, m.sequenceNr: JLong, nowUuid, timeBucket(now),
          m.serialized)
      else {
        val tag1: String = m.tags.head
        preparedWriteMessageWithTag1.bind(persistenceId, maxPnr: JLong, m.sequenceNr: JLong, nowUuid, timeBucket(now),
          tag1, m.serialized)
      }
    }
    // in case we skip an entire partition we want to make sure the empty partition has in in-use flag so scans
    // keep going when they encounter it
    if (partitionNew(firstSeq) && minPnr != maxPnr) writes :+ preparedWriteInUse.bind(persistenceId, minPnr: JLong)
    else writes

  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val logicalDelete = session.executeAsync(preparedInsertDeletedTo.bind(persistenceId, toSequenceNr: JLong))

    val fromSequenceNr = readLowestSequenceNr(persistenceId, 1L)
    val lowestPartition = partitionNr(fromSequenceNr)
    val highestPartition = partitionNr(toSequenceNr) + 1 // may have been moved to the next partition
    val partitionInfos = (lowestPartition to highestPartition).map(partitionInfo(persistenceId, _, toSequenceNr))

    partitionInfos.map(future => future.flatMap(pi => {
      Future.sequence((pi.minSequenceNr to pi.maxSequenceNr).grouped(config.maxMessageBatchSize).map { group =>
        {
          val delete = asyncDeleteMessages(pi.partitionNr, group map (MessageId(persistenceId, _)))
          delete.onFailure {
            case e => log.warning(s"Unable to complete deletes for persistence id ${persistenceId}, toSequenceNr ${toSequenceNr}. The plugin will continue to function correctly but you will need to manually delete the old messages.", e)
          }
          delete
        }
      })
    }))

    logicalDelete.map(_ => ())
  }

  private def partitionInfo(persistenceId: String, partitionNr: Long, maxSequenceNr: Long): Future[PartitionInfo] = {
    session.executeAsync(preparedSelectHighestSequenceNr.bind(persistenceId, partitionNr: JLong))
      .map(rs => Option(rs.one()))
      .map(row => row.map(s => PartitionInfo(partitionNr, minSequenceNr(partitionNr), min(s.getLong("sequence_nr"), maxSequenceNr)))
        .getOrElse(PartitionInfo(partitionNr, minSequenceNr(partitionNr), -1)))
  }

  private def asyncDeleteMessages(partitionNr: Long, messageIds: Seq[MessageId]): Future[Unit] = executeBatch({ batch =>
    messageIds.foreach { mid =>
      batch.add(preparedDeletePermanent.bind(mid.persistenceId, partitionNr: JLong, mid.sequenceNr: JLong))
    }
  }, Some(config.deleteRetries))

  private def executeBatch(body: BatchStatement ⇒ Unit, retries: Option[Int] = None): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]
    retries.foreach(times => batch.setRetryPolicy(new LoggingRetryPolicy(new FixedRetryPolicy(times))))
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  private def execute(stmt: Statement, retries: Option[Int] = None): Future[Unit] = {
    stmt.setConsistencyLevel(writeConsistency)
    retries.foreach(times => stmt.setRetryPolicy(new LoggingRetryPolicy(new FixedRetryPolicy(times))))
    session.executeAsync(stmt).map(_ => ())
  }

  def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / targetPartitionSize

  private def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % targetPartitionSize == 0L

  private def minSequenceNr(partitionNr: Long): Long =
    partitionNr * targetPartitionSize + 1

  private def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  override def postStop(): Unit = {
    session.close()
    session.getCluster().close()
  }

  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])
  private case class Serialized(sequenceNr: Long, serialized: ByteBuffer, tags: Set[String])
  private case class PartitionInfo(partitionNr: Long, minSequenceNr: Long, maxSequenceNr: Long)
}

object CassandraJournal {

  private[cassandra] val timeBucketFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  private[cassandra] def timeBucket(epochTimestamp: Long): String = {
    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochTimestamp), ZoneOffset.UTC)
    time.format(timeBucketFormatter)
  }
}

class FixedRetryPolicy(number: Int) extends RetryPolicy {
  override def onUnavailable(statement: Statement, cl: ConsistencyLevel, requiredReplica: Int, aliveReplica: Int, nbRetry: Int): RetryDecision = retry(cl, nbRetry)
  override def onWriteTimeout(statement: Statement, cl: ConsistencyLevel, writeType: WriteType, requiredAcks: Int, receivedAcks: Int, nbRetry: Int): RetryDecision = retry(cl, nbRetry)
  override def onReadTimeout(statement: Statement, cl: ConsistencyLevel, requiredResponses: Int, receivedResponses: Int, dataRetrieved: Boolean, nbRetry: Int): RetryDecision = retry(cl, nbRetry)

  private def retry(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry < number) RetryDecision.retry(cl) else RetryDecision.rethrow()
  }

  override def init(c: Cluster): Unit = ()
  override def close(): Unit = ()

}
