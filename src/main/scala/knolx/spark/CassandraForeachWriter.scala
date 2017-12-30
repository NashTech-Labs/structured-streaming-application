package knolx.spark

import com.datastax.driver.core.{Cluster, Session}
import knolx.Config.{cassandraHosts, keyspace}
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * Copyright Knoldus Software LLP. All rights reserved.
  */
object CassandraForeachWriter extends Serializable {
  val writeToCassandra = new ForeachWriter[Row] {
    private var cluster: Cluster = _
    private var session: Session = _

    override def process(row: Row): Unit = {
      val word = row.getString(0)
      val count = row.getLong(1)

      session.execute(s"insert into $keyspace.wordcount (word, count) values ('$word', $count);")
    }

    override def close(errorOrNull: Throwable): Unit = {
      session.close()
      session.getCluster.close()
    }

    override def open(partitionId: Long, version: Long): Boolean = {
      cluster = Cluster.builder.addContactPoints(cassandraHosts).build
      session = cluster.newSession()
      true
    }
  }
}
