package knolx.spark

import knolx.Config._
import knolx.KnolXLogger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.types.StructType

/**
  * Copyright Knoldus Inc.. All rights reserved.
  */
object StreamStreamJoiner extends App with KnolXLogger {
  info("Creating Spark Session")
  val spark = SparkSession.builder().master(sparkMaster).appName(sparkAppName).getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  info("Streaming companies Dataframe")
  val companiesDF =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", companiesTopic)
      .load()
      .select(col("value").cast("string").as("companyName"),
        col("timestamp").as("companyTradingTime"))

  companiesDF.writeStream.format("console").option("truncate", false).start()

  info("Original Streaming Dataframe")
  val schema = ScalaReflection.schemaFor[Stock].dataType.asInstanceOf[StructType]
  val stockStreamDF =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", stocksTopic)
      .load()
      .select(from_json(col("value").cast("string"), schema).as("value"),
        col("timestamp").as("stockInputTime"))
      .select("value.*", "stockInputTime")

  info("Filtered Streaming Dataframe")
  val filteredStockStreamDF = stockStreamDF.join(companiesDF,
    expr("companyName = stockName AND stockInputTime >= companyTradingTime AND stockInputTime <= companyTradingTime + interval 20 seconds"))
  val filteredStockStreamingQuery = filteredStockStreamDF.writeStream.format("console").option("truncate", false).start()

  info("Waiting for the query to terminate...")
  filteredStockStreamingQuery.awaitTermination()
  filteredStockStreamingQuery.stop()
}
