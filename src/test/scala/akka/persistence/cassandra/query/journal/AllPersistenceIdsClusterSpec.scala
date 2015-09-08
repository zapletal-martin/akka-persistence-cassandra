package akka.persistence.cassandra.query.journal

import scala.concurrent.Await

import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.journal.CassandraJournalConfiguration
import akka.persistence.query.{PersistenceQuery, NoRefresh, AllPersistenceIds}
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{SocketUtil, ImplicitSender, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpecLike}

object AllPersistenceIdsClusterSpec {
  def config(port: Int): String =
    s"""
       |akka {
       |  loglevel = "OFF"
       |  actor {
       |    provider = "akka.cluster.ClusterActorRefProvider"
       |  }
       |  remote {
       |    netty.tcp {
       |      hostname = 127.0.0.1
       |      port = $port
        |    }
        |  }
        |}
     """.stripMargin

  def clusterConfig(port: Int): String =
    s"""
       |akka {
       |  cluster {
       |    seed-nodes = ["akka.tcp://AllPersistenceIdsClusterSpec@127.0.0.1:$port"]
                                                                                    |  }
                                                                                    |}
     """.stripMargin
}

class AllPersistenceIdsClusterSpec  extends TestKit(ActorSystem("AllPersistenceIdsSpec", CassandraJournalConfiguration.config))
with ImplicitSender //TODO: with Cleanup
with WordSpecLike
with CassandraLifecycle
with Matchers {

  import AllPersistenceIdsClusterSpec._

  implicit val timeout = Timeout(3.second)

  var clusterSystem1: ActorSystem = _
  var clusterSystem2: ActorSystem = _

  override protected def beforeAll() = {
    super.beforeAll()

    val clusterSystem1Port = SocketUtil.temporaryServerAddress().getPort
    val clusterSystem2Port = SocketUtil.temporaryServerAddress().getPort

    clusterSystem1 = ActorSystem("AllPersistenceIdsClusterSpec",
      ConfigFactory.parseString(clusterConfig(clusterSystem1Port))
        .withFallback(ConfigFactory.parseString(config(clusterSystem1Port)))
        .withFallback(CassandraJournalConfiguration.config))

    clusterSystem2 = ActorSystem("AllPersistenceIdsClusterSpec",
      ConfigFactory.parseString(clusterConfig(clusterSystem1Port))
        .withFallback(ConfigFactory.parseString(config(clusterSystem2Port)))
        .withFallback(CassandraJournalConfiguration.config))
  }

  override protected def afterAll() = {
    Await.result(clusterSystem1.terminate(), timeout.duration)
    Await.result(clusterSystem2.terminate(), timeout.duration)
  }

  "Leveldb query AllPersistenceIds cluster" must {
    "find existing persistenceIds" in {
      clusterSystem1.actorOf(TestActor.props("a")) ! "a1"
      expectMsg("a1-done")
      clusterSystem1.actorOf(TestActor.props("b")) ! "b1"
      expectMsg("b1-done")
      clusterSystem1.actorOf(TestActor.props("c")) ! "c1"
      expectMsg("c1-done")

      implicit val mat = ActorMaterializer()(clusterSystem2)
      val queries = PersistenceQuery(clusterSystem2).readJournalFor(CassandraReadJournal.Identifier)

      val src = queries.query(AllPersistenceIds, NoRefresh)
      src.runWith(TestSink.probe[String])
        .request(5)
        .expectNextUnordered("a", "b", "c")
        .expectComplete()
    }

    "find new persistenceIds" in {
      // a, b, c created by previous step
      clusterSystem1.actorOf(TestActor.props("d")) ! "d1"
      expectMsg("d1-done")

      implicit val mat = ActorMaterializer()(clusterSystem2)
      val queries = PersistenceQuery(clusterSystem2).readJournalFor(CassandraReadJournal.Identifier)
      val src = queries.query(AllPersistenceIds)
      val probe = src.runWith(TestSink.probe[String])
        .request(5)
        .expectNextUnorderedN(List("a", "b", "c", "d"))

      clusterSystem1.actorOf(TestActor.props("e")) ! "e1"
      probe.expectNext("e")

      val more = (1 to 100).map("f" + _)
      more.foreach { p ⇒
        clusterSystem1.actorOf(TestActor.props(p)) ! p
      }

      probe.request(100)
      probe.expectNextUnorderedN(more)
    }
  }
}