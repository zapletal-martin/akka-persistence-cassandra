package akka.persistence.cassandra.query.scaladsl

import scala.concurrent.ExecutionContext.Implicits.global

import akka.actor.{Props, ActorSystem}
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Second, Span}
import org.scalatest.{Matchers, WordSpecLike}

import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.query.TestActor
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.journal.{Tagged, WriteEventAdapter}
import akka.persistence.query.PersistenceQuery

object CassandraReadJournalSpec {
  val config = s"""
    akka.loglevel = INFO
    akka.test.single-expect-default = 10s
    akka.persistence.journal.plugin = "cassandra-journal"
    cassandra-journal.port = ${CassandraLauncher.randomPort}
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-journal.event-adapters {
      test-tagger = akka.persistence.cassandra.query.scaladsl.TestTagger
    }
    cassandra-journal.event-adapter-bindings = {
      "java.lang.String" = test-tagger
    }
               """
}

class TestTagger extends WriteEventAdapter {
  override def manifest(event: Any): String = ""
  override def toJournal(event: Any): Any = Tagged(event, Set("a"))
}

class CassandraReadJournalSpec
  extends TestKit(ActorSystem("ScalaCassandraReadJournalSpec", ConfigFactory.parseString(CassandraReadJournalSpec.config)))
  with ScalaFutures
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers {

  override def systemName: String = "ScalaCassandraReadJournalSpec"

  implicit val mat = ActorMaterializer()(system)

  implicit val patience = PatienceConfig(Span(10, Seconds), Span(1, Second))

  lazy val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  "Cassandra Read Journal Scala API" must {
    "start eventsByPersistenceId query" in {
      val a = system.actorOf(Props(new TestActor("a")))
      a ! "a-1"
      expectMsg("a-1-done")

      val src = queries.eventsByPersistenceId("a", 0L, Long.MaxValue)
      src.runWith(Sink.head).map(_.persistenceId).futureValue.shouldEqual("a")
    }

    "start current eventsByPersistenceId query" in {
      val a = system.actorOf(Props(new TestActor("b")))
      a ! "b-1"
      expectMsg("b-1-done")

      val src = queries.currentEventsByPersistenceId("b", 0L, Long.MaxValue)
      src.runWith(Sink.head).map(_.persistenceId).futureValue.shouldEqual("b")
    }

    "start eventsByTag query" in {
      val src = queries.eventsByTag("a", 0L)
      src.runWith(Sink.head).map(_.persistenceId).futureValue.shouldEqual("a")
    }

    "start current eventsByTag query" in {
      val src = queries.currentEventsByTag("a", 0L)
      src.runWith(Sink.head).map(_.persistenceId).futureValue.shouldEqual("a")
    }
  }
}
