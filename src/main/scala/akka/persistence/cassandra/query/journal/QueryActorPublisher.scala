package akka.persistence.cassandra.query.journal

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}

import akka.actor.ActorLogging
import akka.pattern.pipe
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request, SubscriptionTimeoutExceeded}
import akka.util.Timeout

object QueryActorPublisher {
  case object DeliverRequest
}

abstract class QueryActorPublisher[MessageType, State](refreshInterval: Option[FiniteDuration], maxBufSize: Int)
  extends ActorPublisher[MessageType]
  with DeliveryBuffer[MessageType]
  with BufferOperations[MessageType, State]
  with CassandraQuery
  with ActorLogging {

  protected case class Update(state: State)
  protected case class BufferUpdate(newBuf: Vector[MessageType])

  //TODO: FIX
  override protected implicit def databaseQueryTimeout: Timeout = Timeout(1.second)

  //TODO: FIX
  private val timeout = Timeout(1.seconds)

  private val tickTask =
    context.system.scheduler.schedule(timeout.duration, timeout.duration, self, Update)(context.dispatcher)

  private implicit val ec = context.dispatcher

  def receive: Receive = starting

  def streaming(buffer: Vector[MessageType], state: State): Receive = {
    case Update(s) ⇒
      query(s).map(BufferUpdate).pipeTo(self)

    case BufferUpdate(current) ⇒
      val (newBuf, newState) = updateBuf(buffer, current, state)
      println(s"NEWBUF $newBuf")
      println(s"NEWSTATE $newState")
      val rest = deliverBuf(newBuf)
      if (isStreamComplete(rest)) onCompleteThenStop()
      context.become(streaming(rest, newState))
    //self ! DelierRequest

    /*case DelierRequest =>
      val rest = deliverBuf(buffer)
      if (isStreamComplete(buffer)) onCompleteThenStop()
      context.become(streaming(rest, state))*/

    case m: ActorPublisherMessage ⇒ m match {

      case _: Request ⇒
        println("REQUEST2 ")
        val rest = deliverBuf(buffer)
        if (isStreamComplete(buffer)) onCompleteThenStop()
        context.become(streaming(rest, state))

      case Cancel ⇒ context.stop(self)

      case SubscriptionTimeoutExceeded ⇒ context.stop(self)
    }
  }

  val starting: Receive = {
    case  _: Request =>
      println("REQUEST")
      query(initialState).map(BufferUpdate).pipeTo(self)
      //self ! DelierRequest
      context.become(streaming(Vector.empty[MessageType], initialState))
  }

  /*override def preStart(): Unit = {
    super.preStart()
    query(initialState)
  }*/

  private def isStreamComplete(buf: Vector[MessageType]) = !refreshInterval.isDefined && buf.isEmpty

  // TODO: FIX parameters
  protected def query(state: State): Future[Vector[MessageType]]

  protected def initialState: State
}