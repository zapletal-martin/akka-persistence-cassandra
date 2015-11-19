package akka.persistence.cassandra.query

import java.time.LocalDate
import java.time.ZoneOffset
import java.util.UUID

import scala.concurrent.duration._

import akka.actor.ActorLogging
import akka.actor.DeadLetterSuppression
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.persistence.cassandra.journal.CassandraJournal
import akka.persistence.query.EventEnvelope
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorPublisherMessage.Request
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.datastax.driver.core.utils.UUIDs

/**
 * INTERNAL API
 */
private[query] object EventsByTagPublisher {

  def props(tag: String, fromOffset: UUID, refreshInterval: Option[FiniteDuration], maxBufSize: Int,
            session: Session, preparedSelect: PreparedStatement): Props = {
    refreshInterval match {
      case Some(interval) ⇒
        Props(new LiveEventsByTagPublisher(tag, fromOffset, interval,
          maxBufSize, session, preparedSelect))
      case None ⇒
        Props(new CurrentEventsByTagPublisher(tag, fromOffset, maxBufSize, session, preparedSelect))
    }
  }

  private[query] case object Continue

  private[query] final case class TaggedEventEnvelope(
    offset: UUID,
    persistenceId: String,
    sequenceNr: Long,
    event: Any)
    extends DeadLetterSuppression with NoSerializationVerificationNeeded

  private[query] case class ReplayDone(count: Int)
    extends DeadLetterSuppression
  private[query] final case class ReplayFailed(cause: Throwable)
    extends DeadLetterSuppression with NoSerializationVerificationNeeded

}

/**
 * INTERNAL API
 */
private[query] abstract class AbstractEventsByTagPublisher(
  val tag: String, val fromOffset: UUID,
  val maxBufSize: Int, val session: Session, val preparedSelect: PreparedStatement)
  extends ActorPublisher[EventEnvelope] with DeliveryBuffer[EventEnvelope] with ActorLogging {
  import EventsByTagPublisher._

  var currTimeBucket: String = CassandraJournal.timeBucket(UUIDs.unixTimestamp(fromOffset))
  var currOffset: UUID = fromOffset

  def nextTimeBucket(): Unit = {
    val nextDay = LocalDate.parse(currTimeBucket, CassandraJournal.timeBucketFormatter).plusDays(1)
    currTimeBucket = nextDay.format(CassandraJournal.timeBucketFormatter)
  }

  def today: LocalDate = LocalDate.now(ZoneOffset.UTC)

  def bucketDate: LocalDate = LocalDate.parse(currTimeBucket, CassandraJournal.timeBucketFormatter)

  def isTimeBucketBeforeToday(): Boolean =
    bucketDate.isBefore(today)

  def isTimeBucketTodayOrLater(): Boolean = {
    val tday = today
    val bucket = bucketDate
    bucket.isEqual(tday) || bucket.isAfter(tday)
  }

  // exceptions from Fetcher
  override val supervisorStrategy = OneForOneStrategy() {
    case e =>
      self ! ReplayFailed(e)
      SupervisorStrategy.Stop
  }

  def receive = init

  def init: Receive = {
    case _: Request ⇒ receiveInitialRequest()
    case Continue   ⇒ // skip, wait for first Request
    case Cancel     ⇒ context.stop(self)
  }

  def receiveInitialRequest(): Unit

  def idle: Receive = {
    case Continue ⇒
      if (timeForReplay)
        replay()

    case _: Request ⇒
      receiveIdleRequest()

    case Cancel ⇒
      context.stop(self)
  }

  def receiveIdleRequest(): Unit

  def timeForReplay: Boolean =
    (buf.isEmpty || buf.size <= maxBufSize / 2)

  def replay(): Unit = {
    val limit = maxBufSize - buf.size
    log.debug("request replay for tag [{}] from [{}] limit [{}]", tag, currOffset, limit)
    context.actorOf(EventsByTagFetcher.props(tag, currTimeBucket, currOffset, limit, self, session, preparedSelect))
    context.become(replaying(limit))
  }

  def replaying(limit: Int): Receive = {
    case TaggedEventEnvelope(offs, pid, seqNr, evt) ⇒
      // FIXME handle seqNr in wrong order, due to Materialized View async eventually consistency
      buf :+= EventEnvelope(
        offset = UUIDs.unixTimestamp(offs),
        persistenceId = pid,
        sequenceNr = seqNr,
        event = evt)
      currOffset = offs
      deliverBuf()

    case ReplayDone(count) ⇒
      log.debug("replay completed for tag [{}], timBucket [{}], count [{}]", tag, currTimeBucket, count)
      receiveReplayDone(count)

    case ReplayFailed(cause) ⇒
      log.debug("replay failed for tag [{}], due to [{}]", tag, cause.getMessage)
      deliverBuf()
      onErrorThenStop(cause)

    case _: Request ⇒
      deliverBuf()

    case Continue ⇒ // skip during replay

    case Cancel ⇒
      context.stop(self)
  }

  def receiveReplayDone(count: Int): Unit
}

/**
 * INTERNAL API
 */
private[query] class LiveEventsByTagPublisher(
  tag: String, fromOffset: UUID,
  refreshInterval: FiniteDuration,
  maxBufSize: Int, session: Session, preparedSelect: PreparedStatement)
  extends AbstractEventsByTagPublisher(
    tag, fromOffset, maxBufSize, session, preparedSelect) {
  import EventsByTagPublisher._

  val tickTask =
    context.system.scheduler.schedule(refreshInterval, refreshInterval, self, Continue)(context.dispatcher)

  override def postStop(): Unit =
    tickTask.cancel()

  override def receiveInitialRequest(): Unit = {
    replay()
  }

  override def receiveIdleRequest(): Unit = {
    deliverBuf()
  }

  override def receiveReplayDone(count: Int): Unit = {
    deliverBuf()

    // FIXME we might need to stay on previous bucket for a while when midnight?
    if (count == 0 && isTimeBucketBeforeToday())
      nextTimeBucket()

    context.become(idle)
  }

}

private[query] class CurrentEventsByTagPublisher(
  tag: String, fromOffset: UUID, maxBufSize: Int,
  session: Session, preparedSelect: PreparedStatement)
  extends AbstractEventsByTagPublisher(
    tag, fromOffset, maxBufSize, session, preparedSelect) {
  import EventsByTagPublisher._

  var replayDone = false

  override val today: LocalDate = LocalDate.now(ZoneOffset.UTC)

  override def receiveInitialRequest(): Unit =
    replay()

  override def receiveIdleRequest(): Unit = {
    deliverBuf()
    if (buf.isEmpty)
      onCompleteThenStop()
    else
      self ! Continue
  }

  override def timeForReplay: Boolean = !replayDone && super.timeForReplay

  override def receiveReplayDone(count: Int): Unit = {
    deliverBuf()

    if (count == 0 && isTimeBucketTodayOrLater()) {
      replayDone = true
      if (buf.isEmpty)
        onCompleteThenStop()
    } else if (count == 0) {
      nextTimeBucket()
      self ! Continue // more to fetch
    } else {
      self ! Continue // more to fetch
    }

    context.become(idle)
  }

}
