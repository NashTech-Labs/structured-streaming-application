package knolx

import com.datastax.driver.core.Cluster
import knolx.Config.{cassandraHosts, keyspace}

/**
  * Copyright Knoldus Software LLP. All rights reserved.
  */
object StructuredStreamingWordCount extends App {
  val cluster = Cluster.builder.addContactPoints(cassandraHosts).build
  val session = cluster.newSession()

  session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH " +
    "replication = {'class':'SimpleStrategy','replication_factor':1};")

  session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.wordcount ( word text PRIMARY KEY,count int );")
  session.close()
  session.getCluster.close()
}
