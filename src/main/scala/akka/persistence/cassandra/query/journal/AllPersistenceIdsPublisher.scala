package akka.persistence.cassandra.query.journal

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.Props
import akka.pattern.pipe

private[journal] object AllPersistenceIdsPublisher {
  def props(refreshInterval: Option[FiniteDuration], maxBufSize: Int, writeJournalPluginId: String): Props =
    Props(new AllPersistenceIdsPublisher(refreshInterval, maxBufSize))

  case object Update
}

//TODO: Manage multiple AllPersistenceIdsPublisher's stream state in one place
//TODO: Buffer size limit
//TODO: Refresh interval
//TODO: Implementation of diff only works because set idempotence property. Will have to handle for other queries.
//TODO: Free the implementation of buffer from interface. E.g. we want it to be a set rather than vector sometimes.
private[journal] class AllPersistenceIdsPublisher(refreshInterval: Option[FiniteDuration], maxBufSize: Int)
  extends QueryActorPublisherClean[String, Int](refreshInterval, maxBufSize) {

  override protected def query(state: Int): Future[Vector[String]] = {
    implicit val ec = context.dispatcher

    curentAllPersistenceIds(state, state + 50).map(_.toVector)
  }

  override protected def initialState: Int = 0

  override def updateBuf(
      buf: Vector[String],
      newBuf: Vector[String],
      state: Int): (Vector[String], Int) =
    (buf ++ newBuf, state + 50)
}