package knolx

import com.datastax.driver.core.Cluster
import knolx.Config.{cassandraHosts, keyspace, sparkAppName, sparkMaster}
import org.apache.spark.sql.SparkSession

/**
  * Copyright Knoldus Software LLP. All rights reserved.
  */
object StructuredStreamingWordCount extends App with KnolXLogger {
  val cluster = Cluster.builder.addContactPoints(cassandraHosts).build
  val session = cluster.newSession()

  info("Creating Keypsace and tables in Cassandra")
  session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH " +
  "replication = {'class':'SimpleStrategy','replication_factor':1};")

  session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.wordcount ( word text PRIMARY KEY,count int );")

  info("Closing DB connection")
  session.close()
  session.getCluster.close()

  info("Creating Spark Session")
  val spark = SparkSession.builder().master(sparkMaster).appName(sparkAppName).getOrCreate()
}
