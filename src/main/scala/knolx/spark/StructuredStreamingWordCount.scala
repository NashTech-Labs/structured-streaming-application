package knolx.spark

import com.datastax.driver.core.Cluster
import knolx.Config._
import knolx.KnolXLogger
import knolx.spark.CassandraForeachWriter.writeToCassandra
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType

/**
  * Copyright Knoldus Inc.. All rights reserved.
  */
object StructuredStreamingWordCount extends App with KnolXLogger {
  val cluster = Cluster.builder.addContactPoints(cassandraHosts).build
  val session = cluster.newSession()

  info("Creating Keypsace and tables in Cassandra...")
  session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH " +
    "replication = {'class':'SimpleStrategy','replication_factor':1};")

  session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.wordcount ( word text PRIMARY KEY,count int );")

  info("Closing DB connection...")
  session.close()
  session.getCluster.close()

  info("Creating Spark Session")
  val spark = SparkSession.builder().master(sparkMaster).appName(sparkAppName).getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  info("Creating Streaming DF...")
  val dataStream =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", topic)
      .load()

  info("Writing data to Cassandra...")
  val query =
    dataStream
      .select(col("value").cast(StringType).as("word"), lit(1).as("count"))
      .groupBy(col("word"))
      .agg(sum("count").as("count"))
      .writeStream
      .outputMode(OutputMode.Update())
      .foreach(writeToCassandra)
      .option("checkpointLocation", checkPointDir)
      .start()

  info("Waiting for the query to terminate...")
  query.awaitTermination()
  query.stop()
}
