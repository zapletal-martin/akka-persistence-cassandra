package akka.persistence.cassandra.query

trait CassandraReadStatements {

  def config: CassandraReadJournalConfig

  private def eventsByTagViewName = s"${config.keyspace}.${config.eventsByTagView}"
  private def tableName = s"${config.keyspace}.${config.table}"

  def selectEventsByTag(tagId: Int) = s"""
      SELECT * FROM $eventsByTagViewName$tagId WHERE
        tag$tagId = ? AND
        timebucket = ? AND
        timestamp > ? AND
        timestamp < ?
        ORDER BY timestamp ASC
        LIMIT ?
    """

  // TODO: Duplicated in CassandraStatements
  def selectMessages = s"""
      SELECT * FROM $tableName WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
    """

  def selectInUse = s"""
     SELECT used from $tableName WHERE
      persistence_id = ? AND
      partition_nr = ?
   """
}
