package akka.persistence.cassandra.journal

import java.lang.{Long => JLong}

import akka.persistence.cassandra.journal.StreamMerger.{PersistenceId, JournalEntry}
import com.datastax.driver.core.BoundStatement

trait IndexWriter extends CassandraStatements with BatchWriter {

  session.execute(createEventsByPersistenceIdTable)

  val preparedWriteMessage = session.prepare(super.writeEventsByPersistenceId)
  val preparedWriteInUse = session.prepare(writeEventsByPersistenceIdInUse)

  // TODO: Store progress of all persistenceJournals so we can resume?
  def writeIndexProgress(stream: Seq[JournalEntry]): Unit = {
    val byPersistenceId = stream.groupBy(_.persistenceId)

    val boundJournalEntry: (JournalEntry, Long) => BoundStatement =
      (entry, maxPnr) => preparedWriteMessage.bind(
        entry.persistenceId.id,
        maxPnr: JLong,
        entry.sequenceNr: JLong,
        entry.serialized)

    writeBatch[PersistenceId, JournalEntry](
      byPersistenceId,
      _._2.head.sequenceNr,
      _._2.last.sequenceNr,
      boundJournalEntry,
      x => x._2.head.persistenceId.id,
      Some((persistenceId, minPnr) => preparedWriteInUse.bind(persistenceId, minPnr: JLong)))
  }

  /*def writeEventsByPersistenceId(stream: Seq[JournalEntry]): Unit = {
    val byPersistenceId = stream.groupBy(_.persistenceId)
    val boundStatements = byPersistenceId.map(statementGroup)

    val batchStatements = boundStatements.map({ unit =>
      executeBatch(batch => unit.foreach(batch.add))
    })
    val promise = Promise[Seq[Try[Unit]]]()

    Future.sequence(batchStatements).onComplete {
      case Success(_) => promise.complete(Success(Seq()))
      case Failure(e) => promise.failure(e)
    }

    promise.future
  }

  private[this] def statementGroup(atomicWrites: (PersistenceId, Seq[JournalEntry])): Seq[BoundStatement] = {
    val firstSequenceNr = atomicWrites._2.head.sequenceNr
    val lastSequenceNr = atomicWrites._2.last.sequenceNr

    val maxPnr = partitionNr(firstSequenceNr)
    val minPnr = partitionNr(lastSequenceNr)
    val persistenceId: String = atomicWrites._2.head.persistenceId.id
    val all = atomicWrites._2

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    require(maxPnr - minPnr <= 1, "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[BoundStatement] = all.map { m =>
      preparedWriteMessage.bind(
        persistenceId,
        maxPnr: JLong,
        m.journalSequenceNr: JLong,
        persistenceId,
        m.sequenceNr: JLong,
        m.serialized)
    }
    // in case we skip an entire partition we want to make sure the empty partition has in in-use flag so scans
    // keep going when they encounter it
    if (partitionNew(firstSequenceNr) && minPnr != maxPnr) writes :+ preparedWriteInUse.bind(persistenceId, minPnr: JLong)
    else writes
  }

  private[this] def executeBatch(body: BatchStatement ⇒ Unit, retries: Option[Int] = None): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(config.writeConsistency).asInstanceOf[BatchStatement]
    retries.foreach(times => batch.setRetryPolicy(new LoggingRetryPolicy(new FixedRetryPolicy(times))))
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  private[this] def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / config.targetPartitionSize

  private[this] def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % config.targetPartitionSize == 0L*/
}
