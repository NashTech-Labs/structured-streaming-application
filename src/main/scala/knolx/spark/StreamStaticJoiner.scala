package knolx.spark

import knolx.Config._
import knolx.KnolXLogger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType

/**
  * Copyright Knoldus Inc.. All rights reserved.
  */
object StreamStaticJoiner extends App with KnolXLogger {
  info("Creating Spark Session")
  val spark = SparkSession.builder().master(sparkMaster).appName(sparkAppName).getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  info("Static Dataframe")
  val companiesDF = spark.read.option("header", "true").csv("src/main/resources/companies.csv")
  companiesDF.show(false)

  info("Original Streaming Dataframe")
  val schema = ScalaReflection.schemaFor[Stock].dataType.asInstanceOf[StructType]
  val stockStreamDF =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", topic)
      .load()
      .select(from_json(col("value").cast("string"), schema).as("value"))
      .select("value.*")

  stockStreamDF.printSchema()
  stockStreamDF.writeStream.format("console").start()

  info("Filtered Streaming Dataframe")
  val filteredStockStreamDF = stockStreamDF.join(companiesDF, "companyName")
  val filteredStockStreamingQuery = filteredStockStreamDF.writeStream.format("console").start()

  info("Waiting for the query to terminate...")
  filteredStockStreamingQuery.awaitTermination()
  filteredStockStreamingQuery.stop()
}
