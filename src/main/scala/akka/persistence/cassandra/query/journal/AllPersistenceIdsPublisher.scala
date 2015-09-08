package akka.persistence.cassandra.query.journal

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.Props
import akka.pattern.pipe

private[journal] object AllPersistenceIdsPublisher {
  def props(refreshInterval: Option[FiniteDuration], maxBufSize: Int, writeJournalPluginId: String): Props =
    Props(new AllPersistenceIdsPublisher(refreshInterval, maxBufSize, writeJournalPluginId))

  case object Update
}

//TODO: Manage multiple AllPersistenceIdsPublisher's stream state in one place
//TODO: Buffer size limit
//TODO: Refresh interval
//TODO: Implementation of diff only works because set idempotence property. Will have to handle for other queries.
//TODO: Free the implementation of buffer from interface. E.g. we want it to be a set rather than vector sometimes.
private[journal] class AllPersistenceIdsPublisher(refreshInterval: Option[FiniteDuration], maxBufSize: Int, override val writeJournalPluginId: String)
  extends QueryActorPublisher[String, Set[String]](refreshInterval, maxBufSize, writeJournalPluginId) {

  /*override protected def update(params: String): Unit = {
    implicit val ec = context.dispatcher
    curentAllPersistenceIds().map(_.toVector).map(BufferUpdate(_)).pipeTo(self)
  }

  override def updateBuf(buf: Vector[String], newBuf: Vector[String]): Vector[String] = {
    val difference = newBuf.diff(buf)
    buf ++= newIds
  }*/
  override protected def query(state: Set[String]): Future[Vector[String]] = {
    implicit val ec = context.dispatcher
    curentAllPersistenceIds().map(_.toVector)
  }

  override protected def initialState: Set[String] = Set.empty[String]

  override def updateBuf(
    buf: Vector[String],
    newBuf: Vector[String],
    state: Set[String]): (Vector[String], Set[String]) = {

    //TODO: FIX
    val difference = newBuf.toSet.diff(state)
    (buf ++ difference, state ++ difference)
  }
}