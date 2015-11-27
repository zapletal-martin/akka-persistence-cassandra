package akka.persistence.cassandra.query.javadsl

import akka.actor.{Props, ActorSystem}
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.query.{TestActor, javadsl, scaladsl}
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.ExecutionContext.Implicits.global

import akka.persistence.query.PersistenceQuery
import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.ScalaFutures

object CassandraReadJournalSpec {
  val config = s"""
    akka.loglevel = INFO
    akka.test.single-expect-default = 10s
    akka.persistence.journal.plugin = "cassandra-journal"
    cassandra-journal.port = ${CassandraLauncher.randomPort}
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
               """
}

class CassandraReadJournalSpec
  extends TestKit(ActorSystem("ScalaCassandraReadJournalSpec", ConfigFactory.parseString(CassandraReadJournalSpec.config)))
  with ScalaFutures
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers {

  override def systemName: String = "JavaCassandraReadJournalSpec"

  implicit val mat = ActorMaterializer()(system)

  lazy val javaQueries = PersistenceQuery(system)
    .getReadJournalFor(classOf[javadsl.CassandraReadJournal], scaladsl.CassandraReadJournal.Identifier)

  "Cassandra Read Journal Java API" must {
    "start eventsByPersistenceId query" in {
      val a = system.actorOf(Props(new TestActor("a")))
      a ! "a-1"
      expectMsg("a-1-done")

      val src = javaQueries.eventsByPersistenceId("a", 0L, Long.MaxValue)
      src.runWith(Sink.head, mat).map(_.persistenceId).futureValue.shouldEqual("a")
    }

    "start current eventsByPersistenceId query" in {
      val a = system.actorOf(Props(new TestActor("b")))
      a ! "b-1"
      expectMsg("b-1-done")

      val src = javaQueries.currentEventsByPersistenceId("b", 0L, Long.MaxValue)
      src.runWith(Sink.head, mat).map(_.persistenceId).futureValue.shouldEqual("b")
    }
  }
}
