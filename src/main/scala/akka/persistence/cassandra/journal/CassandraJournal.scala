package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import scala.collection.immutable.Seq
import scala.concurrent._

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence._
import akka.persistence.cassandra._
import akka.serialization.SerializationExtension

import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes

import scala.util.{Success, Failure, Try}

class CassandraJournal extends AsyncWriteJournal with CassandraRecovery with CassandraStatements {

  val config = new CassandraJournalConfig(context.system.settings.config.getConfig("cassandra-journal"))
  val serialization = SerializationExtension(context.system)

  import config._

  val cluster = clusterBuilder.build
  val session = cluster.connect()

  case class MessageId(persistenceId: String, sequenceNr: Long)

  if (config.keyspaceAutoCreate) {
    retry(config.keyspaceAutoCreateRetries) {
      session.execute(createKeyspace)
    }
  }
  session.execute(createTable)

  val preparedWriteMessage = session.prepare(writeMessage)
  val preparedConfirmMessage = session.prepare(confirmMessage)
  val preparedDeletePermanent = session.prepare(deleteMessage)
  val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)
  val preparedCheckInUse = session.prepare(selectInUse).setConsistencyLevel(readConsistency)
  val preparedWriteInUse = session.prepare(writeInUse)

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    // we need to preserve the order / size of this sequence even though we don't map
    // AtomicWrites 1:1 with a C* insert
    val serialized = messages.map(aw => Try { SerializedAtomicWrite(
        aw.payload.head.persistenceId,
        aw.payload.map(pr => Serialized(pr.sequenceNr, persistentToByteBuffer(pr))))
    })
    val result = serialized.map(a => a.map(_ => ()))

    val byPersistenceId = serialized.collect({ case Success(caw) => caw }).groupBy(_.persistenceId).values
    val boundStatements = byPersistenceId.map(statementGroup)

    val batchStatements = boundStatements.map({ unit =>
        executeBatch(batch => unit.foreach(batch.add))
    })
    val promise: Promise[Seq[Try[Unit]]] = Promise[Seq[Try[Unit]]]()

    Future.sequence(batchStatements).onComplete {
      case Success(_) => promise.complete(Success(result))
      case Failure(e) => promise.failure(e)
    }

    promise.future
  }

  private def statementGroup(atomicWrites: Seq[SerializedAtomicWrite]): Seq[BoundStatement] = {
    val maxPnr = partitionNr(atomicWrites.last.payload.last.sequenceNr)
    val firstSeq: JLong = atomicWrites.head.payload.head.sequenceNr
    val minPnr: JLong = partitionNr(firstSeq)
    val persistenceId: String = atomicWrites.head.persistenceId
    val all = atomicWrites.flatMap(_.payload)

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    require(maxPnr - minPnr <= 1, "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[BoundStatement] = all.map { m =>
      preparedWriteMessage.bind(persistenceId, maxPnr: JLong, m.sequenceNr: JLong, m.serialized)
    }
    // in case we skip an entire partition we want to make sure the empty partition has in in-use flag so scans
    // keep going when they encounter it
    if (partitionNew(firstSeq) && minPnr != maxPnr) writes :+ preparedWriteInUse.bind(persistenceId, minPnr)
    else writes

  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val fromSequenceNr = readLowestSequenceNr(persistenceId, 1L)
    val asyncDeletions = (fromSequenceNr to toSequenceNr).grouped(persistence.settings.journal.maxDeletionBatchSize / 2).map { group =>
      asyncDeleteMessages(group map (MessageId(persistenceId, _)))
    }
    Future.sequence(asyncDeletions).map(_ => ())
  }

  private def asyncDeleteMessages(messageIds: Seq[MessageId]): Future[Unit] = executeBatch { batch =>
    messageIds.foreach { mid =>
      val firstPnr: JLong = partitionNr(mid.sequenceNr)
      val stmt = preparedDeletePermanent.bind(mid.persistenceId, firstPnr: JLong, mid.sequenceNr: JLong)
      // the message could be in next partition as a result of an AtomicWrite, alternative is a read before write
      val stmt2 = preparedDeletePermanent.bind(mid.persistenceId, firstPnr+1: JLong, mid.sequenceNr: JLong)
      batch.add(stmt)
      batch.add(stmt2)
    }
  }

  private def executeBatch(body: BatchStatement ⇒ Unit): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / targetPartitionSize

  private def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % targetPartitionSize == 0L

  private def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  override def postStop(): Unit = {
    session.close()
    cluster.close()
  }

  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])
  private case class Serialized(sequenceNr: Long, serialized: ByteBuffer)

}
