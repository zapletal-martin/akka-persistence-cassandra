package akka.persistence.cassandra.query

import scala.annotation.tailrec
import scala.concurrent.Future

import akka.pattern.pipe
import akka.actor._
import com.datastax.driver.core.{BoundStatement, PagingState, Row, ResultSet}

import akka.persistence.cassandra._
import akka.persistence.cassandra.query.AllPersistenceIdsPublisher._
import akka.persistence.cassandra.query.AllPersistenceIdsFetcher._

private[query] object AllPersistenceIdsFetcher {
  private sealed trait Action
  private final case class StreamResultSet(count: Long, rs: ResultSet) extends Action
  private case object Exhausted extends Action
  private final case class Finished(pagingState: ResultSet) extends Action

  def props(
      replyTo: ActorRef,
      session: AllPersistenceIdsSession,
      max: Long,
      pagingState: Option[ResultSet],
      settings: CassandraReadJournalConfig): Props =
    Props(
      new AllPersistenceIdsFetcher(replyTo, session, max, pagingState, settings))
      .withDispatcher(settings.pluginDispatcher)
}

class AllPersistenceIdsFetcher(
    replyTo: ActorRef,
    session: AllPersistenceIdsSession,
    max: Long,
    pagingState: Option[ResultSet],
    settings: CassandraReadJournalConfig) extends Actor {

  import context.dispatcher

  override def preStart(): Unit = {
    println(s"STARTING NEW FETCHER WITH $pagingState")
    pagingState.fold(prepareQuery())(prepareQuery).pipeTo(self)
  }

  override def receive: Receive = {
    case a: Action => a match {
      case StreamResultSet(c, rs) =>
        continue(c, rs).pipeTo(self)
      case Finished(ps) =>
        println("FINISHED")
        replyTo ! ReplayDone(Some(ps))
        context.stop(self)
      case Exhausted =>
        println("EXHAUSTED")
        replyTo ! ReplayDone(None)
        context.stop(self)
    }

    case Status.Failure(e) =>
      println("BOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOM")
      throw e
  }

  private[this] def prepareQuery(): Future[StreamResultSet] = {
    val boundStatement = session.selectDistinctPersistenceIds.bind()
    boundStatement.setFetchSize(settings.fetchSize)

    query(boundStatement)
  }

  private[this] def prepareQuery(pagingState: ResultSet): Future[StreamResultSet] = {
    /*val boundStatement = session.selectDistinctPersistenceIds.bind()
    boundStatement.setFetchSize(settings.fetchSize)
    boundStatement.setPagingState(pagingState)*/

    //query(boundStatement)

    Future.successful(StreamResultSet(0, pagingState))
  }

  private[this] def query(boundStatement: BoundStatement): Future[StreamResultSet] =
    listenableFutureToFuture(session.session.executeAsync(boundStatement))
      .map(StreamResultSet(0, _))

  private[this] def extractor(row: Row): String = row.getString("persistence_id")

  private[this] def continue(count: Long, resultSet: ResultSet): Future[Action] = {
    if(resultSet.isExhausted) {
      Future.successful(Exhausted)
    } else if(count >= max) {
      Future.successful(Finished(resultSet))
    } else {
      val available = resultSet.getAvailableWithoutFetching
      val rs = exhaustFetch(resultSet, available)
      listenableFutureToFuture(rs.fetchMoreResults()).map(StreamResultSet(count + available, _))
    }
  }

  @tailrec private def exhaustFetch(resultSet: ResultSet, n: Int): ResultSet = {
    if(n == 0) {
      resultSet
    } else {
      val row = resultSet.one()
      replyTo ! extractor(row)
      exhaustFetch(resultSet, n - 1)
    }
  }
}
