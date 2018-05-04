package knolx.spark

import com.datastax.spark.connector.cql.CassandraConnector
import knolx.Config.keyspace
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * Copyright Knoldus Software LLP. All rights reserved.
  */
object CassandraForeachWriter extends Serializable {
  def writeToCassandra(cassandraConnector: CassandraConnector) = new ForeachWriter[Row] {
    override def process(row: Row): Unit = {
      val word = row.getString(0)
      val count = row.getLong(1)

      cassandraConnector.withSessionDo(_.execute(s"insert into $keyspace.wordcount (word, count) values ('$word', $count);"))
    }

    override def close(errorOrNull: Throwable): Unit = {}

    override def open(partitionId: Long, version: Long): Boolean =
      true
  }
}
