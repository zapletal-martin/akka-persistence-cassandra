package akka.persistence.cassandra.query.journal

import scala.collection.immutable.HashSet

/**
 * Cassandra backed store of all PersistenceIds in the journal.
 * Keeps track of all the currently known PersistenceIds
 * to know what to deliver to subscribers.
 */
private[journal] trait AllPersistenceIdsStore {

  private var ids: Set[String] = HashSet.empty[String]

  def allPersistenceIds: Set[String] = ids

  def diff(newIds: Set[String]) = {
    println("-------")
    println(s"ids: $ids, new: $newIds, diff: ${newIds.diff(ids)}")
    println("-------")
    newIds.diff(ids)
  }

  protected def addPersistenceIds(newIds: Set[String]): Unit = ids ++= newIds
}
