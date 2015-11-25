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
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

/**
 * INTERNAL API
 */
private[query] object EventsByTagPublisher {

  def props(tag: String, fromOffset: UUID, refreshInterval: Option[FiniteDuration], settings: CassandraReadJournalConfig,
            session: Session, preparedSelect: PreparedStatement): Props = {
    refreshInterval match {
      case Some(interval) ⇒
        Props(new LiveEventsByTagPublisher(tag, fromOffset, interval,
          settings, session, preparedSelect))
      case None ⇒
        Props(new CurrentEventsByTagPublisher(tag, fromOffset, settings, session, preparedSelect))
    }
  }

  private[query] case object Continue

  private[query] final case class TaggedEventEnvelope(
    offset: UUID,
    persistenceId: String,
    sequenceNr: Long,
    event: Any)
    extends DeadLetterSuppression with NoSerializationVerificationNeeded

  private[query] case class ReplayDone(count: Int, seqNumbers: SequenceNumbers)
    extends DeadLetterSuppression
  private[query] case class ReplayAborted(
    seqNumbers: SequenceNumbers, persistenceId: String, expectedSeqNr: Long, gotSeqNr: Long)
    extends DeadLetterSuppression
  private[query] final case class ReplayFailed(cause: Throwable)
    extends DeadLetterSuppression with NoSerializationVerificationNeeded

}

/**
 * INTERNAL API
 */
private[query] abstract class AbstractEventsByTagPublisher(
  val tag: String, val fromOffset: UUID,
  val settings: CassandraReadJournalConfig, val session: Session, val preparedSelect: PreparedStatement)
  extends ActorPublisher[EventEnvelope] with DeliveryBuffer[EventEnvelope] with ActorLogging {
  import EventsByTagPublisher._
  import settings.maxBufferSize

  val eventualConsistencyDelayMillis = settings.eventualConsistencyDelay.toMillis

  var currTimeBucket: String = CassandraJournal.timeBucket(UUIDs.unixTimestamp(fromOffset))
  var currOffset: UUID = fromOffset
  var seqNumbers = SequenceNumbers.empty
  var abortDeadline: Option[Deadline] = None

  def nextTimeBucket(): Unit = {
    val nextDay = LocalDate.parse(currTimeBucket, CassandraJournal.timeBucketFormatter).plusDays(1)
    currTimeBucket = nextDay.format(CassandraJournal.timeBucketFormatter)
  }

  def today(): LocalDate =
    LocalDateTime.now(ZoneOffset.UTC).minus(eventualConsistencyDelayMillis, ChronoUnit.MILLIS).toLocalDate

  def bucketDate: LocalDate = LocalDate.parse(currTimeBucket, CassandraJournal.timeBucketFormatter)

  def isTimeBucketBeforeToday(): Boolean =
    bucketDate.isBefore(today())

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
    (buf.isEmpty || buf.size <= maxBufferSize / 2)

  def replay(): Unit = {
    val limit = maxBufferSize - buf.size
    log.debug("query for tag [{}] from [{}] [{}] limit [{}]", tag, currTimeBucket, currOffset, limit)
    val toOffset = UUIDs.endOf(System.currentTimeMillis() - eventualConsistencyDelayMillis)
    context.actorOf(EventsByTagFetcher.props(tag, currTimeBucket, currOffset, toOffset, limit, self,
      session, preparedSelect, seqNumbers))
    context.become(replaying(limit))
  }

  def replaying(limit: Int): Receive = {
    case TaggedEventEnvelope(offs, pid, seqNr, evt) ⇒
      buf :+= EventEnvelope(
        offset = UUIDs.unixTimestamp(offs),
        persistenceId = pid,
        sequenceNr = seqNr,
        event = evt)
      currOffset = offs
      deliverBuf()

    case ReplayDone(count, seqN) ⇒
      log.debug("query chunk done for tag [{}], timBucket [{}], count [{}]", tag, currTimeBucket, count)
      seqNumbers = seqN
      abortDeadline = None
      receiveReplayDone(count)

    case ReplayAborted(seqN, pid, expectedSeqNr, gotSeqNr) ⇒
      seqNumbers = seqN
      def logMsg = s"query chunk aborted for tag [$tag], timBucket [$currTimeBucket], " +
        s" expected sequence number [$expectedSeqNr] for [$pid], but got [$gotSeqNr]"
      abortDeadline match {
        case Some(deadline) if deadline.isOverdue =>
          val m = logMsg
          log.error(m)
          onErrorThenStop(new IllegalStateException(m))
        case _ =>
          if (log.isDebugEnabled) log.debug(logMsg)
          if (abortDeadline.isEmpty)
            abortDeadline = Some(Deadline.now + settings.wrongSequenceNumberOrderTimeout)
          receiveReplayAborted()
      }

    case ReplayFailed(cause) ⇒
      log.debug("query failed for tag [{}], due to [{}]", tag, cause.getMessage)
      deliverBuf()
      onErrorThenStop(cause)

    case _: Request ⇒
      deliverBuf()

    case Continue ⇒ // skip during replay

    case Cancel ⇒
      context.stop(self)
  }

  def receiveReplayDone(count: Int): Unit

  def receiveReplayAborted(): Unit
}

/**
 * INTERNAL API
 */
private[query] class LiveEventsByTagPublisher(
  tag: String, fromOffset: UUID,
  refresh: FiniteDuration,
  settings: CassandraReadJournalConfig, session: Session, preparedSelect: PreparedStatement)
  extends AbstractEventsByTagPublisher(
    tag, fromOffset, settings, session, preparedSelect) {
  import EventsByTagPublisher._

  val tickTask =
    context.system.scheduler.schedule(refresh, refresh, self, Continue)(context.dispatcher)

  override def postStop(): Unit =
    tickTask.cancel()

  override def receiveInitialRequest(): Unit =
    replay()

  override def receiveIdleRequest(): Unit =
    deliverBuf()

  override def receiveReplayDone(count: Int): Unit = {
    deliverBuf()

    if (count == 0 && isTimeBucketBeforeToday()) {
      nextTimeBucket()
      self ! Continue
    }

    context.become(idle)
  }

  // will retry by the scheduled Continue
  override def receiveReplayAborted(): Unit =
    context.become(idle)

}

private[query] class CurrentEventsByTagPublisher(
  tag: String, fromOffset: UUID, settings: CassandraReadJournalConfig,
  session: Session, preparedSelect: PreparedStatement)
  extends AbstractEventsByTagPublisher(
    tag, fromOffset, settings, session, preparedSelect) {
  import EventsByTagPublisher._
  import context.dispatcher

  var replayDone = false

  override val today: LocalDate = super.today()
  val endTime = System.currentTimeMillis() + settings.eventualConsistencyDelay.toMillis

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

    if (count == 0) {
      if (isTimeBucketBeforeToday()) {
        nextTimeBucket()
        self ! Continue // more to fetch
      } else if (System.currentTimeMillis() > endTime) {
        replayDone = true
        if (buf.isEmpty)
          onCompleteThenStop()
      } else {
        // in the eventual consistency delay zone
        context.system.scheduler.scheduleOnce(settings.refreshInterval, self, Continue)
      }
    } else {
      self ! Continue // more to fetch
    }

    context.become(idle)
  }

  override def receiveReplayAborted(): Unit = {
    context.system.scheduler.scheduleOnce(settings.refreshInterval, self, Continue)
    context.become(idle)
  }

}
