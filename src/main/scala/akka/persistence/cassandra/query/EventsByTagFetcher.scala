package akka.persistence.cassandra.query

import java.nio.ByteBuffer
import java.util.UUID

import scala.concurrent.Future

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Status
import akka.pattern.pipe
import akka.persistence.PersistentRepr
import akka.serialization.SerializationExtension
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import com.datastax.driver.core.utils.Bytes

private[query] object EventsByTagFetcher {

  private final case class InitResultSet(rs: ResultSet)
  private case object Fetched

  def props(tag: String, timeBucket: String, fromOffset: UUID, limit: Int, replyTo: ActorRef,
            session: Session, preparedSelect: PreparedStatement): Props =
    Props(new EventsByTagFetcher(tag, timeBucket, fromOffset, limit, replyTo, session, preparedSelect))

}

private[query] class EventsByTagFetcher(tag: String, timeBucket: String, fromOffset: UUID, limit: Int, replyTo: ActorRef,
                                        session: Session, preparedSelect: PreparedStatement) extends Actor {

  import context.dispatcher
  import akka.persistence.cassandra.listenableFutureToFuture
  import EventsByTagFetcher._
  import EventsByTagPublisher._

  val serialization = SerializationExtension(context.system)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  var highestOffset: UUID = fromOffset
  var count = 0

  override def preStart(): Unit = {
    val boundStmt = preparedSelect.bind(tag, timeBucket, fromOffset, limit: Integer)
    // FIXME tune fetch size
    //            boundStmt.setFetchSize(2)
    val init: Future[ResultSet] = session.executeAsync(boundStmt)
    init.map(InitResultSet.apply).pipeTo(self)
  }

  def receive = {
    case InitResultSet(rs) =>
      context.become(active(rs))
      continue(rs)
    case Status.Failure(e) =>
      // from pipeTo
      throw e
  }

  def active(resultSet: ResultSet): Receive = {
    case Fetched =>
      continue(resultSet)
    case Status.Failure(e) =>
      // from pipeTo
      throw e
  }

  def continue(resultSet: ResultSet): Unit = {
    if (resultSet.isExhausted()) {
      replyTo ! ReplayDone(count)
      context.stop(self)
    } else {
      var n = resultSet.getAvailableWithoutFetching
      while (n > 0) {
        n -= 1
        val row = resultSet.one()
        val pid = row.getString("persistence_id")
        val seqNr = row.getLong("sequence_nr")
        val m = persistentFromByteBuffer(row.getBytes("message"))
        val offs = row.getUUID("timestamp")
        highestOffset = offs
        count += 1
        val eventEnvelope = TaggedEventEnvelope(
          offset = offs,
          persistenceId = pid,
          sequenceNr = row.getLong("sequence_nr"),
          event = m.payload)
        replyTo ! eventEnvelope
      }
      val more: Future[ResultSet] = resultSet.fetchMoreResults()
      more.map(_ => Fetched).pipeTo(self)
    }
  }

  // FIXME remove
  def printAll(): Unit = {
    val iter = session.execute(s"SELECT * FROM akka.eventsbytag1").iterator
    while (iter.hasNext()) {
      val row = iter.next()
      val pid = row.getString("persistence_id")
      val seqNr = row.getLong("sequence_nr")
      val m = persistentFromByteBuffer(row.getBytes("message"))
      val offs = row.getUUID("timestamp")
      val buck = row.getString("timebucket")
      println(s"# replay ALL $buck $pid $seqNr $offs (${offs.timestamp}) ${m.payload}  [${row.getString("tag1")}]") // FIXME
    }
  }

}
