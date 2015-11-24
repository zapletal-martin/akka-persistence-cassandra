package akka.persistence.cassandra.query

import java.lang.{ Long => JLong }
import scala.concurrent.duration._
import akka.persistence.journal.Tagged
import akka.persistence.query.EventEnvelope
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.ImplicitSender
import akka.persistence.journal.WriteEventAdapter
import akka.testkit.TestKit
import akka.persistence.cassandra.CassandraLifecycle
import org.scalatest.Matchers
import akka.actor.ActorSystem
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import org.scalatest.WordSpecLike
import com.typesafe.config.ConfigFactory
import akka.persistence.PersistentActor
import akka.actor.Props
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.stream.testkit.TestSubscriber
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.CassandraStatements
import akka.persistence.PersistentRepr
import java.util.UUID
import akka.persistence.cassandra.journal.CassandraJournal
import akka.serialization.SerializationExtension
import java.time.LocalDateTime
import java.time.ZoneOffset
import com.datastax.driver.core.utils.UUIDs
import java.nio.ByteBuffer
import java.util.TimeZone
import java.time.ZoneId
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.util.Try

object EventsByTagSpec {
  val today = LocalDate.now(ZoneOffset.UTC)

  val config = ConfigFactory.parseString(s"""
    akka.loglevel = DEBUG
    akka.test.single-expect-default = 10s
    akka.persistence.journal.plugin = "cassandra-journal"
    cassandra-journal {
      #target-partition-size = 5
      port = ${CassandraLauncher.randomPort}
      event-adapters {
        color-tagger  = akka.persistence.cassandra.query.ColorFruitTagger
      }
      event-adapter-bindings = {
        "java.lang.String" = color-tagger
      }
      tags {
        green = 1
        black = 1
        blue = 1
        pink = 1
        apple = 2
        T1 = 1
      }
    }
    cassandra-query-journal {
      refresh-interval = 1s
      max-buffer-size = 50
      first-time-bucket = ${today.minusDays(5).format(CassandraJournal.timeBucketFormatter)}
    }
    """)

  object TestActor {
    def props(persistenceId: String): Props =
      Props(new TestActor(persistenceId))
  }

  class TestActor(override val persistenceId: String) extends PersistentActor {

    val receiveRecover: Receive = {
      case evt: String ⇒
    }

    val receiveCommand: Receive = {
      case cmd: String ⇒
        persist(cmd) { evt ⇒
          sender() ! evt + "-done"
        }
    }

  }

  // TODO this will be supported in akka-stream 2.0
  implicit class ProbeOps(val probe: TestSubscriber.Probe[Any]) {
    def expectNextPF[T](f: PartialFunction[Any, T]): T = {
      val next = probe.expectNext()
      assert(f.isDefinedAt(next))
      f(next)
    }
  }

}

class ColorFruitTagger extends WriteEventAdapter {
  val colors = Set("green", "black", "blue")
  val fruits = Set("apple", "banana")
  override def toJournal(event: Any): Any = event match {
    case s: String ⇒
      val colorTags = colors.foldLeft(Set.empty[String])((acc, c) ⇒ if (s.contains(c)) acc + c else acc)
      val fruitTags = fruits.foldLeft(Set.empty[String])((acc, c) ⇒ if (s.contains(c)) acc + c else acc)
      val tags = colorTags union fruitTags
      if (tags.isEmpty) event
      else Tagged(event, tags)
    case _ ⇒ event
  }

  override def manifest(event: Any): String = ""
}

class EventsByTagSpec extends TestKit(ActorSystem("EventsByTagSpec", EventsByTagSpec.config))
  with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {
  import EventsByTagSpec._

  override def systemName: String = "EventsByTagSpec"

  implicit val mat = ActorMaterializer()(system)

  lazy val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  val serialization = SerializationExtension(system)
  val writePluginConfig = new CassandraJournalConfig(system.settings.config.getConfig("cassandra-journal"))

  lazy val session = {
    val cluster = writePluginConfig.clusterBuilder.build
    cluster.connect()
  }

  lazy val preparedWriteMessage = {
    val writeStatements: CassandraStatements = new CassandraStatements {
      def config: CassandraJournalConfig = writePluginConfig
    }
    session.prepare(writeStatements.writeMessage)
  }

  def writeTestEvent(time: LocalDateTime, persistent: PersistentRepr, tags: Set[String]): Unit = {
    val serialized = ByteBuffer.wrap(serialization.serialize(persistent).get)
    val timestamp = time.toInstant(ZoneOffset.UTC).toEpochMilli

    val bs = preparedWriteMessage.bind()
    bs.setString("persistence_id", persistent.persistenceId)
    bs.setLong("partition_nr", 1L)
    bs.setLong("sequence_nr", persistent.sequenceNr)
    bs.setUUID("timestamp", uuid(timestamp))
    bs.setString("timebucket", CassandraJournal.timeBucket(timestamp))
    tags.foreach { tag =>
      writePluginConfig.tags.get(tag) match {
        case Some(tagId) => bs.setString("tag" + tagId, tag)
        case None        => throw new IllegalArgumentException("Unknown tag [{}].")
      }
    }
    bs.setBytes("message", serialized)
    session.execute(bs)
  }

  def uuid(timestamp: Long): UUID = {
    def makeMsb(time: Long): Long = {
      // copied from UUIDs.makeMsb

      // UUID v1 timestamp must be in 100-nanoseconds interval since 00:00:00.000 15 Oct 1582.
      val uuidEpoch = LocalDateTime.of(1582, 10, 15, 0, 0).atZone(ZoneId.of("GMT-0")).toInstant.toEpochMilli
      val timestamp = (time - uuidEpoch) * 10000

      var msb = 0L
      msb |= (0x00000000ffffffffL & timestamp) << 32
      msb |= (0x0000ffff00000000L & timestamp) >>> 16
      msb |= (0x0fff000000000000L & timestamp) >>> 48
      msb |= 0x0000000000001000L // sets the version to 1.
      msb
    }

    val now = UUIDs.timeBased()
    new UUID(makeMsb(timestamp), now.getLeastSignificantBits)
  }

  override protected def afterAll(): Unit = {
    Try(session.close())
    super.afterAll()
  }

  "Cassandra query EventsByTag" must {
    "implement standard EventsByTagQuery" in {
      queries.isInstanceOf[EventsByTagQuery] should ===(true)
    }

    "find existing events" in {
      val a = system.actorOf(TestActor.props("a"))
      val b = system.actorOf(TestActor.props("b"))
      a ! "hello"
      expectMsg(s"hello-done")
      a ! "a green apple"
      expectMsg(s"a green apple-done")
      b ! "a black car"
      expectMsg(s"a black car-done")
      a ! "a green banana"
      expectMsg(s"a green banana-done")
      b ! "a green leaf"
      expectMsg(s"a green leaf-done")

      // TODO await materialized view is populated

      val greenSrc = queries.currentEventsByTag(tag = "green", offset = 0L)
      val probe = greenSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 3L, "a green banana") => e }
      probe.expectNoMsg(500.millis)
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe.expectComplete()

      val blackSrc = queries.currentEventsByTag(tag = "black", offset = 0L)
      val probe2 = blackSrc.runWith(TestSink.probe[Any])
      probe2.request(5)
      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "a black car") => e }
      probe2.expectComplete()

      val appleSrc = queries.currentEventsByTag(tag = "apple", offset = 0L)
      val probe3 = appleSrc.runWith(TestSink.probe[Any])
      probe3.request(5)
      probe3.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe3.expectComplete()
    }

    "complete when no events" in {
      val src = queries.currentEventsByTag(tag = "pink", offset = 0L)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectComplete()
    }

    "not see new events after demand request" in {
      val c = system.actorOf(TestActor.props("c"))

      val greenSrc = queries.currentEventsByTag(tag = "green", offset = 0L)
      val probe = greenSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 3L, "a green banana") => e }
      probe.expectNoMsg(100.millis)

      c ! "a green cucumber"
      expectMsg(s"a green cucumber-done")

      probe.expectNoMsg(100.millis)
      probe.request(5)
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe.expectComplete() // green cucumber not seen
    }

    "find events from offset" in {
      val greenSrc1 = queries.currentEventsByTag(tag = "green", offset = 0L)
      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
      probe1.request(2)
      probe1.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      val offs = probe1.expectNextPF { case e @ EventEnvelope(_, "a", 3L, "a green banana") => e }.offset
      probe1.cancel()

      val greenSrc2 = queries.currentEventsByTag(tag = "green", offs)
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      probe2.expectNextPF { case e @ EventEnvelope(_, "a", 3L, "a green banana") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "c", 1L, "a green cucumber") => e }
      probe2.expectComplete()
    }

    "find existing events that spans several time buckets" in {
      val t1 = today.minusDays(5).atStartOfDay.plusHours(13)
      val w1 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("e1", 1L, "p1", "", writerUuid = w1)
      writeTestEvent(t1, pr1, Set("T1"))
      val t2 = t1.plusHours(1)
      val pr2 = PersistentRepr("e2", 2L, "p1", "", writerUuid = w1)
      writeTestEvent(t2, pr2, Set("T1"))
      val t3 = t1.plusDays(1)
      val pr3 = PersistentRepr("e3", 3L, "p1", "", writerUuid = w1)
      writeTestEvent(t3, pr3, Set("T1"))
      val t4 = t1.plusDays(3)
      val pr4 = PersistentRepr("e4", 4L, "p1", "", writerUuid = w1)
      writeTestEvent(t4, pr4, Set("T1"))

      val src = queries.currentEventsByTag(tag = "T1", offset = 0L)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "e2") => e }
      probe.expectNoMsg(500.millis)
      probe.request(5)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 3L, "e3") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 4L, "e4") => e }
      probe.expectComplete()
    }
  }

  "Cassandra live query EventsByTag" must {
    "find new events" in {
      val d = system.actorOf(TestActor.props("d"))

      val blackSrc = queries.eventsByTag(tag = "black", offset = 0L)
      val probe = blackSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "a black car") => e }
      probe.expectNoMsg(100.millis)

      d ! "a black dog"
      expectMsg(s"a black dog-done")
      d ! "a black night"
      expectMsg(s"a black night-done")

      probe.expectNextPF { case e @ EventEnvelope(_, "d", 1L, "a black dog") => e }
      probe.expectNoMsg(100.millis)
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "d", 2L, "a black night") => e }
      probe.cancel()
    }

    "find events from offset" in {
      val greenSrc1 = queries.eventsByTag(tag = "green", offset = 0L)
      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
      probe1.request(2)
      probe1.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      val offs = probe1.expectNextPF { case e @ EventEnvelope(_, "a", 3L, "a green banana") => e }.offset
      probe1.cancel()

      val greenSrc2 = queries.eventsByTag(tag = "green", offs)
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      probe2.expectNextPF { case e @ EventEnvelope(_, "a", 3L, "a green banana") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "c", 1L, "a green cucumber") => e }
      probe2.expectNoMsg(100.millis)
      probe2.cancel()
    }

    "find new events that spans several time buckets" in {
      val t1 = today.minusDays(5).atStartOfDay.plusHours(13)
      val w1 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("e1", 1L, "p1", "", writerUuid = w1)
      writeTestEvent(t1, pr1, Set("T1"))
      val t2 = t1.plusHours(1)
      val pr2 = PersistentRepr("e2", 2L, "p1", "", writerUuid = w1)
      writeTestEvent(t2, pr2, Set("T1"))

      val src = queries.eventsByTag(tag = "T1", offset = 0L)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "e2") => e }

      val t3 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val pr3 = PersistentRepr("e3", 3L, "p1", "", writerUuid = w1)
      writeTestEvent(t3, pr3, Set("T1"))
      val t4 = LocalDateTime.now(ZoneOffset.UTC)
      val pr4 = PersistentRepr("e4", 4L, "p1", "", writerUuid = w1)
      writeTestEvent(t4, pr4, Set("T1"))

      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 3L, "e3") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 4L, "e4") => e }
      probe.cancel()
    }

    "stream many events" in {
      val e = system.actorOf(TestActor.props("e"))

      val src = queries.eventsByTag(tag = "green", offset = 0L)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(4)
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 3L, "a green banana") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "c", 1L, "a green cucumber") => e }

      for (n <- 1 to 100)
        e ! s"green-$n"

      probe.request(200)
      for (n <- 1 to 100) {
        val Expected = s"green-$n"
        probe.expectNextPF { case e @ EventEnvelope(_, "e", _, Expected) => e }
      }
      probe.expectNoMsg(100.millis)

      for (n <- 101 to 200)
        e ! s"green-$n"

      for (n <- 101 to 200) {
        val Expected = s"green-$n"
        probe.expectNextPF { case e @ EventEnvelope(_, "e", _, Expected) => e }
      }
      probe.expectNoMsg(100.millis)

      probe.request(10)
      probe.expectNoMsg(100.millis)
    }

  }

}
