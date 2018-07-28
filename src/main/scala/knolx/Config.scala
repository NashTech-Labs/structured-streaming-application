package knolx

/**
  * Copyright Knoldus Inc.. All rights reserved.
  */
object Config {
  val bootstrapServer = System.getenv("BOOTSTRAP_SERVERS_CONFIG")
  val topic = System.getenv("TOPIC")
  val cassandraHosts = System.getenv("CASSANDRA_HOSTS")
  val keyspace = System.getenv("CASSANDRA_KEYSPACE")
  val sparkMaster = System.getenv("SPARK_MASTER")
  val sparkAppName = System.getenv("SPARK_APP_NAME")
  val checkPointDir = System.getenv("CHECKPOINT_DIR")
}
