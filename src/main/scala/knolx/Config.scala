package knolx

/**
  * Copyright Knoldus Inc.. All rights reserved.
  */
object Config {
  val bootstrapServer = Option(System.getenv("BOOTSTRAP_SERVERS_CONFIG")).getOrElse("localhost:9092")
  val topic = Option(System.getenv("TOPIC")).getOrElse("data")
  val companiesTopic = System.getenv("COMPANIES_TOPIC")
  val stocksTopic = System.getenv("STOCKS_TOPIC")
  val cassandraHosts = System.getenv("CASSANDRA_HOSTS")
  val keyspace = System.getenv("CASSANDRA_KEYSPACE")
  val sparkMaster = Option(System.getenv("SPARK_MASTER")).getOrElse("local")
  val sparkAppName = Option(System.getenv("SPARK_APP_NAME")).getOrElse("stream")
  val checkPointDir = Option(System.getenv("CHECKPOINT_DIR")).getOrElse("/tmp/demo")
}
