package knolx

import com.datastax.driver.core.{Cluster, Session}
import knolx.Config.{cassandraHosts, keyspace}

/**
  * Copyright Knoldus Software LLP. All rights reserved.
  */
object StructuredStreamingWordCount extends App {
  var session: Session = _
  try {
    val cluster = Cluster.builder.addContactPoints(cassandraHosts).build
    cluster.newSession().execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy','replication_factor':1};")

    session = cluster.connect(keyspace)
    session.execute("CREATE TABLE IF NOT EXISTS wordcount ( word text PRIMARY KEY,count int );")
  } finally {
    session.close()
    session.getCluster.close()
  }
}
