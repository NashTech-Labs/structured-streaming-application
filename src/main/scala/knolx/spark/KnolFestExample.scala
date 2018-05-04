package knolx.spark

import knolx.Config._
import knolx.KnolLogger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Encoders, SparkSession}

/**
  * Copyright Knoldus, Inc. 2018. All rights reserved.
  */
case class CurrentPowerConsumption(kwh: Double)

case class PowerConsumptionStatus(numOfReadings: Long, total: Double, avg: Double, status: String) {
  def compute(newReadings: List[Double]) = {
    val newTotal = newReadings.sum + total
    val newNumOfReadings = numOfReadings + newReadings.size
    val newAvg = newTotal / newNumOfReadings.toDouble

    PowerConsumptionStatus(newNumOfReadings, newTotal, newAvg, "ON")
  }
}

object KnolFestExample extends App with KnolLogger {
  info("Creating Spark Session")
  val spark = SparkSession.builder().master(sparkMaster).appName(sparkAppName).getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val updateStateFunc =
    (deviceId: String, newReadings: Iterator[(String, CurrentPowerConsumption)], state: GroupState[PowerConsumptionStatus]) => {
      val data = newReadings.toList.map { case(_, reading) => reading }.map(_.kwh)

      lazy val initialPowerConsumptionStatus = PowerConsumptionStatus(0L, 0D, 0D, "OFF")
      val currentState = state.getOption.fold(initialPowerConsumptionStatus.compute(data))(_.compute(data))

      val currentStatus =
        if(state.hasTimedOut) {
          // If we do not receive any reading, for a device, we will assume that it is OFF.
          currentState.copy(status = "OFF")
        } else {
          state.setTimeoutDuration("10 seconds")
          currentState
        }

      state.update(currentStatus)
      (deviceId, currentStatus)
    }

  info("Creating Streaming DF...")
  val dataStream =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", topic)
      .option("includeTimestamp", true)
      .load()

  info("Writing data to Console...")
  import spark.implicits._

  implicit val currentPowerConsumptionEncoder = Encoders.kryo[CurrentPowerConsumption]
  implicit val powerConsumptionStatusEncoder = Encoders.kryo[PowerConsumptionStatus]

  val query =
    dataStream
      .select(col("key").cast(StringType).as("key"), col("value").cast(StringType).as("key"))
      .as[(String, String)]
      .map { case(deviceId, unit) =>
        (deviceId, CurrentPowerConsumption(Option(unit).fold(0D)(_.toDouble)))
      }
      .groupByKey { case(deviceId, _) => deviceId }
      .mapGroupsWithState[PowerConsumptionStatus, (String, PowerConsumptionStatus)](GroupStateTimeout.ProcessingTimeTimeout())(updateStateFunc)
      .toDF("deviceId", "current_status")
      .writeStream
      .format("console")
      .option("truncate", false)
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", checkPointDir)
      .start()

  info("Waiting for the query to terminate...")
  query.awaitTermination()
  query.stop()
}
