package akka.persistence.cassandra.query.journal

import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.journal.CassandraJournalConfiguration
import akka.persistence.query.EventsByPersistenceId
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.RefreshInterval
import akka.persistence.query.journal.leveldb.LeveldbReadJournal
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ImplicitSender, TestKit}
import akka.persistence.query.NoRefresh
import akka.persistence.query.AllPersistenceIds
import com.typesafe.config.ConfigFactory
import org.apache.cassandra.tools.NodeTool.Cleanup
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.persistence.query.EventsByPersistenceId
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.RefreshInterval
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.persistence.query.NoRefresh
import akka.persistence.query.AllPersistenceIds

object AllPersistenceIdsSpec {
  val config = """
    akka.loglevel = INFO
    akka.persistence.journal.plugin = "cassandra-journal"
    akka.test.single-expect-default = 10s
               """
}

class AllPersistenceIdsSpec
  extends TestKit(ActorSystem("AllPersistenceIdsSpec", ConfigFactory.parseString(AllPersistenceIdsSpec.config)))
  with ImplicitSender //TODO: with Cleanup
  with WordSpecLike
  with CassandraLifecycle
  with Matchers {

  val secondSystem = ActorSystem("Second", ConfigFactory.parseString(AllPersistenceIdsSpec.config))
  implicit val mat = ActorMaterializer()(secondSystem)

  val queries = PersistenceQuery(secondSystem).readJournalFor(CassandraReadJournal.Identifier)

  "Leveldb query AllPersistenceIds" must {
    "find existing persistenceIds" in {
      system.actorOf(TestActor.props("a")) ! "a1"
      expectMsg("a1-done")
      system.actorOf(TestActor.props("b")) ! "b1"
      expectMsg("b1-done")
      system.actorOf(TestActor.props("c")) ! "c1"
      expectMsg("c1-done")

      val src = queries.query(AllPersistenceIds, NoRefresh)
      src.runWith(TestSink.probe[String])
        .request(5)
        .expectNextUnordered("a", "b", "c")
        .expectComplete()
    }

   /* "find new persistenceIds" in {
      // a, b, c created by previous step
      system.actorOf(TestActor.props("d")) ! "d1"
      expectMsg("d1-done")

      val src = queries.query(AllPersistenceIds)
      val probe = src.runWith(TestSink.probe[String])
        .request(5)
        .expectNextUnorderedN(List("a", "b", "c", "d"))

      system.actorOf(TestActor.props("e")) ! "e1"
      probe.expectNext("e")

      val more = (1 to 100).map("f" + _)
      more.foreach { p ⇒
        system.actorOf(TestActor.props(p)) ! p
      }

      probe.request(100)
      probe.expectNextUnorderedN(more)

    }*/
  }

}