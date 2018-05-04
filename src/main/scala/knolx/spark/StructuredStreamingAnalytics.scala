package knolx.spark

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import knolx.Config._
import knolx.KnolLogger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, OutputMode}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Encoders, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JDouble, JInt, JObject, JString}
import org.json4s.jackson.JsonMethods

/**
  * Copyright Knoldus, Inc. All rights reserved.
  */
sealed trait ComputeStats

case class NumericStatistics(colName: String, oldMin: Double, oldMax: Double, oldSum: Double, oldCount: Int, mean: Double) extends ComputeStats {
  def computeStats(values: List[Double]) = {
    val newMin = if (values.min < oldMin) values.min else oldMin
    val newMax = if (values.max > oldMax) values.max else oldMax
    val newSum = values.sum + oldSum
    val newCount = oldCount + values.size
    val newMean = newSum / newCount.toDouble

    NumericStatistics(colName, newMin, newMax, newSum, newCount, newMean)
  }
}

case class StringStatistics(colName: String, oldCount: Int) extends ComputeStats {
  def computeStats(values: List[String]) = {
    val newCount = oldCount + values.size

    StringStatistics(colName, newCount)
  }
}

case class TimeStampStatistics(colName: String, oldMin: Long, oldMax: Long, oldCount: Int) extends ComputeStats {
  def computeStats(values: List[Long]) = {
    val newCount = oldCount + values.size
    val newMin = if (values.min < oldMin) values.min else oldMin
    val newMax = if (values.max > oldMax) values.max else oldMax

    TimeStampStatistics(colName, newMin, newMax, newCount)
  }
}

object StructuredStreamingAnalytics extends App with KnolLogger {
  info("Creating Spark Session")
  val spark = SparkSession.builder().master(sparkMaster).appName(sparkAppName).getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val statsFunc =
    (key: String, it: Iterator[(String, String)], state: GroupState[List[(String, ComputeStats)]]) => {
      val data = it.toList.map { case(_, value) => value }

      val parsedData = data flatMap { json =>
        implicit val format = DefaultFormats

        val jObj = JsonMethods.parse(json)

        jObj match {
          case JObject(l) =>
            l map { case (colKey, colData) =>
              colData match {
                case JDouble(v) => (colKey, v)
                case JInt(v) => (colKey, v)
                case JString(v) =>
                  try {
                    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    (colKey, LocalDateTime.parse(v, formatter))
                  } catch {
                    case _: Exception =>
                      (colKey, v)
                  }
              }
            }
        }
      }

      val groupedData = parsedData.groupBy{case(colKey, _) => colKey}.map { case(colKey, list) => (colKey, list.map { case(_, v) => v})}.toList

      groupedData foreach { case(colKey, colData) =>
        colData match {
          case x :: _ if(x.isInstanceOf[Double]) =>
            val doubles = colData.map(_.asInstanceOf[Double])
            val oldState = if(state.exists) state.get else Nil
            val currentState = oldState.find { case(colName, _) => colName == colKey}
            val newState = oldState diff List(currentState.getOrElse((colKey, NumericStatistics(colKey, doubles.min, doubles.max, doubles.sum, doubles.size, (doubles.sum/doubles.size)))))
            val oldStats =
              if(currentState.isDefined) {
                currentState.map { case(_, stats) => stats.asInstanceOf[NumericStatistics] }
              } else {
                Some(NumericStatistics(colKey, doubles.min, doubles.max, doubles.sum, doubles.size, (doubles.sum/doubles.size)))
              }

            oldStats
              .map ( _.computeStats(doubles))
              .foreach(newStats => state.update(newState ++ List((colKey, newStats))))

          case x :: _ if(x.isInstanceOf[String]) =>
            val strings = colData.map(_.asInstanceOf[String])
            val oldState = if(state.exists) state.get else Nil
            val currentState = oldState.find { case(colName, _) => colName == colKey}
            val newState = oldState diff List(currentState.getOrElse((colKey, StringStatistics(colKey, strings.size))))
            val oldStats =
              if(currentState.isDefined) {
                currentState.map { case(_, stats) => stats.asInstanceOf[StringStatistics] }
              } else {
                Some(StringStatistics(colKey, strings.size))
              }

            oldStats
              .map ( _.computeStats(strings))
              .foreach(newStats => state.update(newState ++ List((colKey, newStats))))

          case x :: _ if(x.isInstanceOf[LocalDateTime]) =>
            val dts = colData.map(_.asInstanceOf[LocalDateTime]).map(_.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
            val oldState = if(state.exists) state.get else Nil
            val currentState = oldState.find { case(colName, _) => colName == colKey}
            val newState = oldState diff List(currentState.getOrElse((colKey, TimeStampStatistics(colKey, dts.min, dts.max, dts.size))))
            val oldStats =
              if(currentState.isDefined) {
                currentState.map { case(_, stats) => stats.asInstanceOf[TimeStampStatistics] }
              } else {
                Some(TimeStampStatistics(colKey, dts.min, dts.max, dts.size))
              }

            oldStats
              .map ( _.computeStats(dts))
              .foreach(newStats => state.update(newState ++ List((colKey, newStats))))
        }
      }

      if(state.exists) (key, state.get.toString()) else (key, Nil.toString())
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
  import spark.implicits._

  implicit val listEncoder = Encoders.kryo[List[(String, ComputeStats)]]

  val query =
    dataStream
      .select(col("key").cast(StringType).as("key"), col("value").cast(StringType).as("col"))
      .as[(String, String)]
      .groupByKey { case(key, _) => key }
      .mapGroupsWithState[List[(String, ComputeStats)], (String, String)](statsFunc)
      .toDF("key", "value")
      .select(col("key"), col("value").cast(StringType).as("value"))
      .writeStream
      .format("console")
      .option("truncate", false)
      .outputMode(OutputMode.Update())
      .start()

  info("Waiting for the query to terminate...")
  query.awaitTermination()
  query.stop()
}
