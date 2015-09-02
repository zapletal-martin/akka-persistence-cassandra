package akka.persistence.cassandra.query.journal

import akka.stream.actor.ActorPublisher

/**
 * INTERNAL API
 */
private[journal] trait DeliveryBuffer[T] { _: ActorPublisher[T] ⇒

  def deliverBuf(buf: Vector[T]): Vector[T] =
    if (buf.nonEmpty && totalDemand > 0) {
      println("A")
      if (buf.size == 1) {
        // optimize for this common case
        println("B")
        onNext(buf.head)
        Vector.empty[T]
      } else if (totalDemand <= Int.MaxValue) {
        println("C")
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        use foreach onNext
        keep
      } else {
        println("D")
        buf foreach onNext
        Vector.empty[T]
      }
    } else {
      println("E")
      buf
    }
}