/*
package akka.persistence.cassandra.query

import akka.actor.{ActorRef, ActorSystem}
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.time.{Seconds, Second, Span}
import org.scalatest.{Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

import akka.stream.testkit.scaladsl.TestSink

object FunSpec {
  val config = s"""
    akka.loglevel = INFO
    akka.test.single-expect-default = 10s
    akka.persistence.journal.plugin = "cassandra-journal"
    cassandra-journal.port = ${CassandraLauncher.randomPort}
    cassandra-journal.target-partition-size = 5
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s

               """
}

class FunSpec
  extends TestKit(ActorSystem("ScalaCassandraReadJournalSpec", ConfigFactory.parseString(FunSpec.config)))
  with ScalaFutures
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers {

  override def systemName: String = "FunSpec"

  implicit val mat = ActorMaterializer()(system)

  lazy val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  def setup(persistenceId: String, n: Int): ActorRef = {
    val ref = system.actorOf(TestActor.props(persistenceId))
    for(i <- 1 to n) {
      ref ! s"$persistenceId-$i"
      expectMsg(s"$persistenceId-$i-done")
    }

    ref
  }

  "Cassandra query EventsByPersistenceId" must {
    "find existing events" in {
      val ref = setup("a", 500)

      /*val src = queries.currentEventsByPersistenceId("a", 3L, Long.MaxValue)
      src.map{x => println(x); x.event}.runWith(TestSink.probe[Any])
        .request(4)
        .expectNext("a-3", "a-4", "a-5", "a-6")
        .expectNoMsg(500.millis)
        .request(4)
        .expectNext("a-7", "a-8", "a-9", "a-10")
        .expectComplete()*/

      implicit val pc = PatienceConfig(Span(10, Seconds), Span(10, Seconds))
      val src = queries.currentEventsByPersistenceId("a", 3L, Long.MaxValue)
      val result = src.runWith(Sink.foreach(println))
      result.futureValue(pc)
    }
  }
}
*/
