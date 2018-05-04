package knolx

/**
  * Copyright Knoldus Software LLP. All rights reserved.
  */
object Config {
  val bootstrapServer = Option(System.getenv("BOOTSTRAP_SERVERS_CONFIG")).getOrElse("localhost:9092")
  val topic = Option(System.getenv("TOPIC")).getOrElse("knolx")
  val cassandraHosts = Option(System.getenv("CASSANDRA_HOSTS")).getOrElse("localhost")
  val cassandraPort = Option(System.getenv("CASSANDRA_PORT")).getOrElse("9042")
  val keyspace = Option(System.getenv("CASSANDRA_KEYSPACE")).getOrElse("knolx")
  val sparkMaster = Option(System.getenv("SPARK_MASTER")).getOrElse("local")
  val sparkAppName = Option(System.getenv("SPARK_APP_NAME")).getOrElse("knolx")
  val checkPointDir = Option(System.getenv("CHECKPOINT_DIR")).getOrElse("/tmp/knolx")
}
