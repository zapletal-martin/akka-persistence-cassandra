package akka.persistence.cassandra.query.journal

import scala.concurrent.Future

import akka.actor.Actor
import akka.util.Timeout

import scala.concurrent.ExecutionContext.Implicits.global

//TODO: Replace with actual implementation
trait CassandraQuery { _: Actor =>

  protected implicit def databaseQueryTimeout: Timeout

  def curentAllPersistenceIds(from: Int, to: Int): Future[Set[String]] = {
    Future {
      Set("a", "b", "c", "d", "e", "f", "g")
    }
  }
}