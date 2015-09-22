package akka.persistence.cassandra.query.journal

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

import akka.actor.{Actor, ActorLogging}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Request, SubscriptionTimeoutExceeded, Cancel}
import akka.util.Timeout

import akka.pattern.pipe

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

abstract class QueryActorPublisherClean[MessageType, State](
    refreshInterval: Option[FiniteDuration],
    maxBufSize: Int)
  extends ActorPublisher[MessageType]
  with DeliveryBuffer[MessageType]
  with BufferOperations[MessageType, State]
  with CassandraQuery
  with ActorLogging {

  case class More(buf: Vector[MessageType], state: State)

  override def preStart() = {
    context.become(streaming(Vector.empty[MessageType], initialState))
    super.preStart()
  }

  //TODO: FIX
  override protected implicit def databaseQueryTimeout: Timeout = Timeout(1.second)

  //TODO: FIX
  private val timeout = Timeout(1.seconds)


  override def receive: Receive = Actor.emptyBehavior

  /*protected def checkDone(filled: Vector[MessageType], remaining: Vector[MessageType]): Unit = {
    if (filled.isEmpty && remaining.isEmpty) onCompleteThenStop()
  }*/

  protected def checkNeedMore(state: State)(implicit ec: ExecutionContext): Unit = {
    if (totalDemand > 0) query(state).map(More(_, state)).pipeTo(self)
  }

  private def isStreamComplete(buf: Vector[MessageType]) = !refreshInterval.isDefined && buf.isEmpty

  def streaming(buffer: Vector[MessageType], state: State): Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded =>
      context.stop(self)
    case Request(_) =>
      val remaining = deliverBuf(buffer)
      checkNeedMore(state)
      context.become(streaming(remaining, state))
    case More(newBuf, newState) =>
      val (newBuf2, newState2) = updateBuf(buffer, newBuf, state)
      val remaining = deliverBuf(newBuf2)
      checkNeedMore(newState2)
      isStreamComplete(remaining)
      context.become(streaming(remaining, newState2))
  }

  protected def query(state: State): Future[Vector[MessageType]]

  protected def initialState: State
}
