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
import akka.actor.ActorLogging

private[query] object EventsByTagFetcher {

  private final case class InitResultSet(rs: ResultSet)
  private case object Fetched

  def props(tag: String, timeBucket: String, fromOffset: UUID, toOffset: UUID, limit: Int, replyTo: ActorRef,
            session: Session, preparedSelect: PreparedStatement, seqNumbers: SequenceNumbers): Props =
    Props(new EventsByTagFetcher(tag, timeBucket, fromOffset, toOffset, limit, replyTo, session, preparedSelect,
      seqNumbers))

}

private[query] class EventsByTagFetcher(
  tag: String, timeBucket: String, fromOffset: UUID, toOffset: UUID, limit: Int,
  replyTo: ActorRef, session: Session, preparedSelect: PreparedStatement,
  seqN: SequenceNumbers)
  extends Actor with ActorLogging {

  import context.dispatcher
  import akka.persistence.cassandra.listenableFutureToFuture
  import akka.persistence.cassandra.query.UUIDComparator.comparator.compare
  import EventsByTagFetcher._
  import EventsByTagPublisher._

  val serialization = SerializationExtension(context.system)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  var highestOffset: UUID = fromOffset
  var count = 0
  var seqNumbers = seqN

  override def preStart(): Unit = {
    val boundStmt = preparedSelect.bind(tag, timeBucket, fromOffset, toOffset, limit: Integer)
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
      replyTo ! ReplayDone(count, seqNumbers)
      context.stop(self)
    } else {
      var n = resultSet.getAvailableWithoutFetching
      var abort = false
      while (n > 0 && !abort) {
        n -= 1
        val row = resultSet.one()
        val pid = row.getString("persistence_id")
        val seqNr = row.getLong("sequence_nr")

        seqNumbers.isNext(pid, seqNr) match {
          case SequenceNumbers.Yes | SequenceNumbers.PossiblyFirst =>
            seqNumbers = seqNumbers.updated(pid, seqNr)
            val m = persistentFromByteBuffer(row.getBytes("message"))
            val offs = row.getUUID("timestamp")
            if (compare(offs, highestOffset) <= 0)
              log.debug("Events were not ordered by timestamp. Consider increasing eventual-consistency-delay " +
                "if the order is of importance.")
            highestOffset = offs
            count += 1
            val eventEnvelope = TaggedEventEnvelope(
              offset = offs,
              persistenceId = pid,
              sequenceNr = row.getLong("sequence_nr"),
              event = m.payload)
            replyTo ! eventEnvelope

          case SequenceNumbers.After =>
            abort = true
            replyTo ! ReplayAborted(seqNumbers, pid, seqNumbers.get(pid) + 1, seqNr)

          case SequenceNumbers.Before =>
            // duplicate, discard
            log.debug(s"Discarding duplicate. Got sequence number [$seqNr] for [$pid], " +
              s"but current sequence number is [${seqNumbers.get(pid)}]")
        }

      }

      if (!abort) {
        val more: Future[ResultSet] = resultSet.fetchMoreResults()
        more.map(_ => Fetched).pipeTo(self)
      }
    }
  }

  // FIXME remove
  def printAll(): Unit = {
    val iter = session.execute(s"SELECT * FROM akka.eventsbytag3").iterator
    while (iter.hasNext()) {
      val row = iter.next()
      val pid = row.getString("persistence_id")
      val seqNr = row.getLong("sequence_nr")
      val m = persistentFromByteBuffer(row.getBytes("message"))
      val offs = row.getUUID("timestamp")
      val buck = row.getString("timebucket")
      println(s"# replay ALL $buck $pid $seqNr $offs (${offs.timestamp}) ${m.payload}  [${row.getString("tag3")}]") // FIXME
    }

    val iter2 = session.execute(s"SELECT * FROM akka.messages").iterator
    while (iter2.hasNext()) {
      val row = iter2.next()
      val pid = row.getString("persistence_id")
      val seqNr = row.getLong("sequence_nr")
      val m = persistentFromByteBuffer(row.getBytes("message"))
      val offs = row.getUUID("timestamp")
      val buck = row.getString("timebucket")
      println(s"# replay ALL MESSAGES $buck $pid $seqNr $offs (${offs.timestamp}) ${m.payload}  [${row.getString("tag3")}]") // FIXME
    }

  }

}
