/*
package akka.persistence.cassandra.query

import java.lang.{Long => JLong}

import akka.stream.scaladsl.Source
import com.google.common.util.concurrent.Futures

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import akka.persistence.cassandra._
import akka.persistence.query.EventEnvelope
import com.datastax.driver.core.{Row, ResultSet, Session, PreparedStatement}

import scala.concurrent.Future

trait EventsByPersistenceIdFetcher {

  type Batch = Seq[Row]
  type Partition = Seq[Future[Batch]]

  def query
      (persistenceId: String, from: Long, to: Long, targetPartitionSize: Int)
      (settings: CassandraReadJournalConfig, session: Session, preparedSelectMessages: PreparedStatement,
      preparedCheckInUse: PreparedStatement, preparedSelectDeletedTo: PreparedStatement): Seq[Future[Partition]] = {

    highestDeletedSequenceNumber(persistenceId, preparedSelectDeletedTo, session).map { h =>
      val initialFromSequenceNr = math.max(h + 1, from)

    }

  }

  def querySinglePartition
      (persistenceId: String, from: Long, to: Long, targetPartitionSize: Int, partitionNr: Long)
      (settings: CassandraReadJournalConfig, session: Session, preparedSelectMessages: PreparedStatement,
      preparedCheckInUse: PreparedStatement, preparedSelectDeletedTo: PreparedStatement): Future[Partition] = {

    val inUse: Future[ResultSet] =
      listenableFutureToFuture(session.executeAsync(preparedCheckInUse.bind(persistenceId, partitionNr: JLong)))

    inUse.flatMap { use =>

      // TODO: Use used
      val used = if (use.isExhausted){
        false
      } else {
        use.one().getBool("used")
      }

      if(used) {
        val initial: Future[ResultSet] = listenableFutureToFuture(
          session.executeAsync(
            preparedSelectMessages.bind(
              persistenceId,
              partitionNr: JLong,
              from: JLong,
              to: JLong)))

        val next: Future[Seq[Future[Seq[Row]]]] = initial.map { i =>
          val immediatelyAvailable = Future(loop(i, Seq(), i.getAvailableWithoutFetching))
          val all = extract(i, Seq())

          val x = immediatelyAvailable +: all
          x
        }

        next
      } else {
        Future(Seq())
      }
    }
  }

  @tailrec private[this] def extract(rs: ResultSet, results: Seq[Future[Seq[Row]]]): Seq[Future[Seq[Row]]] = {
    if(rs.isExhausted) {
      results
    } else {
      extract(rs, results :+ listenableFutureToFuture(rs.fetchMoreResults()).map(r => loop(r, Seq(), r.getAvailableWithoutFetching)))
    }
  }

  @tailrec private[this] def loop(rs: ResultSet, results: Seq[Row], available: Int): Seq[Row] = {
    if(available == 0) {
      results
    } else {
      loop(rs, results :+ rs.one(), available - 1)
    }
  }

  private[this] def highestDeletedSequenceNumber(partitionKey: String, preparedSelectDeletedTo: PreparedStatement, session: Session): Future[Long] = {
    listenableFutureToFuture(session.executeAsync(preparedSelectDeletedTo.bind(partitionKey)))
      .map(r => Option(r.one()).map(_.getLong("deleted_to")).getOrElse(0))
  }

  private def partitionNr(sequenceNr: Long, targetPartitionSize: Int): Long =
    (sequenceNr - 1L) / targetPartitionSize
}
*/
