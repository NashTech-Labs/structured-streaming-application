package knolx.spark

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnectorConf._
import knolx.Config._
import knolx.KnolLogger
import knolx.spark.CassandraForeachWriter.writeToCassandra
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType

/**
  * Copyright Knoldus Software LLP. All rights reserved.
  */
object StructuredStreamingWordCount extends App with KnolLogger {
  info("Creating Spark Session")
  val spark = SparkSession.builder().master(sparkMaster).appName(sparkAppName).getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val cassandraConnector = {
    val sparkConf = {
      val options = ConnectionHostParam.option(cassandraHosts) ++ ConnectionPortParam.option(cassandraPort)
      val conf = spark.sqlContext.sparkContext.getConf
      for ((k, v) <- options) conf.set(k, v)
      conf
    }
    CassandraConnector(sparkConf)
  }

  info("Creating Keypsace and tables in Cassandra...")
  cassandraConnector.withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy','replication_factor':1};")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.wordcount ( word text PRIMARY KEY,count int );")
  }

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
      .foreach(writeToCassandra(cassandraConnector))
      .option("checkpointLocation", checkPointDir)
      .start()

  info("Waiting for the query to terminate...")
  query.awaitTermination()
  query.stop()
}
