package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }

import akka.actor.ActorLogging

import scala.concurrent._

import com.datastax.driver.core.{ResultSet, Row}

import akka.persistence.PersistentRepr

trait CassandraRecovery extends ActorLogging {
  this: CassandraJournal =>
  import config._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = {
    log.debug("asyncReplyMessages {} {} {} {}", persistenceId, fromSequenceNr, toSequenceNr, max)
    Future {
      replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max)(replayCallback)
    }
  }

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("asyncReadHighestSequenceNr {} {}", persistenceId, fromSequenceNr)
    Future {
      readHighestSequenceNr(persistenceId, fromSequenceNr)
    }
  }

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    log.debug("readHighestSequenceNr {} {}", persistenceId, fromSequenceNr)
    val result = new MessageIterator(persistenceId, math.max(1L, fromSequenceNr), Long.MaxValue, Long.MaxValue).foldLeft(fromSequenceNr) { case (acc, msg) => msg.sequenceNr }
    log.debug(s"Highest sequence number $result")
    result
  }

  def readLowestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    log.debug("readLowestSequenceNr {} {}", persistenceId, fromSequenceNr)
    new MessageIterator(persistenceId, fromSequenceNr, Long.MaxValue, Long.MaxValue).find(!_.deleted).map(_.sequenceNr).getOrElse(fromSequenceNr)
  }

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Unit = {
    log.debug("replayMessages {} {} {} {}", persistenceId, fromSequenceNr, toSequenceNr, max)
    new MessageIterator(persistenceId, fromSequenceNr, toSequenceNr, max).foreach( msg => {
      log.debug(s"Replaying $msg")
      replayCallback(msg)
    })
  }

  /**
   * Iterator over messages, crossing partition boundaries.
   */
  class MessageIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long) extends Iterator[PersistentRepr] {

    import PersistentRepr.Undefined

    private val iter = new RowIterator(persistenceId, fromSequenceNr, toSequenceNr)
    private var mcnt = 0L

    private var c: PersistentRepr = null
    private var n: PersistentRepr = PersistentRepr(Undefined)

    fetch()

    def hasNext: Boolean =
      n != null && mcnt < max

    def next(): PersistentRepr = {
      fetch()
      mcnt += 1
      c
    }

    /**
     * Make next message n the current message c, complete c
     * and pre-fetch new n.
     */
    private def fetch(): Unit = {
      c = n
      n = null
      while (iter.hasNext && n == null) {
        val row = iter.next()
        val snr = row.getLong("sequence_nr")
        val m = persistentFromByteBuffer(row.getBytes("message"))
        // there may be duplicates returned by iter
        // (on scan boundaries within a partition)
        if (snr == c.sequenceNr) c = m else n = m
      }
    }
  }

  /**
   * Iterates over rows, crossing partition boundaries.
   */
  class RowIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[Row] {
    var currentPnr = partitionNr(fromSequenceNr)
    var currentSnr = fromSequenceNr

    var fromSnr = fromSequenceNr
    var toSnr = toSequenceNr

    var rcnt = 0
    var iter = newIter()

    def newIter() = {
      log.debug(s"id=$persistenceId partition=$currentPnr from=$fromSnr to=$toSnr")
      session.execute(preparedSelectMessages.bind(persistenceId, currentPnr: JLong, fromSnr: JLong, toSnr: JLong)).iterator
    }

    def inUse: Boolean = {
      val execute: ResultSet = session.execute(preparedCheckInUse.bind(persistenceId, currentPnr: JLong))
      if (execute.isExhausted) {
        false
      } else {
        val used = execute.one().getBool("used")
        log.debug(s"In use partition $currentPnr : $used")
        used
      }

    }

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (iter.hasNext) {
        // more entries available in current resultset
        true
      } else if (rcnt == 0 && !inUse) {
        // empty partition and inUse not set, empty partitions are a result of deletions or large persistAll calls
        log.debug(s"Empty partition and not in use $currentPnr")
        false
      } else if (rcnt < maxResultSize) {
        // all entries consumed, try next partition
        currentPnr += 1
        log.debug(s"Moving to partition $currentPnr")
        fromSnr = currentSnr
        rcnt = 0
        iter = newIter()
        hasNext
      } else {
        // max result set size reached, continue with same partition moving the fromSnr forward
        log.debug(s"Max results size reached, moving along a partition $currentPnr")
        fromSnr = currentSnr
        rcnt = 0
        iter = newIter()
        hasNext
      }
    }

    def next(): Row = {
      val row = iter.next()
      currentSnr = row.getLong("sequence_nr")
      rcnt += 1
      row
    }

    private def sequenceNrMin(partitionNr: Long): Long =
      partitionNr * maxPartitionSize + 1L

    private def sequenceNrMax(partitionNr: Long): Long =
      (partitionNr + 1L) * maxPartitionSize
  }
}
