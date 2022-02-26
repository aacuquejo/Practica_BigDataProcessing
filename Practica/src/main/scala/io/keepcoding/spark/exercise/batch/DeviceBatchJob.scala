package io.keepcoding.spark.exercise.batch
import io.keepcoding.spark.exercise.streaming.DeviceStreamingJob.{enrichDevicesWithMetadata, spark}
import org.apache.spark.sql.functions.{lit, sum, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

object DeviceBatchJob extends BatchJob {
  override val spark: SparkSession  = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {

    spark
      .read
      .format("parquet")
      .load(s"${storagePath}")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

  override def readDeviceMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichDeviceWithMetadata(deviceDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    deviceDF.as("device")
      .join(
        metadataDF.as("metadata"),
        $"device.id" === $"metadata.id"
      ).drop($"metadata.id")
  }


  override def DevicesCountBytesAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp",$"antenna_id".alias("id"),$"bytes" )
      .groupBy($"id",window($"timestamp","1 hour"))
      .agg(
        sum("bytes").as("value")

      )
      .withColumn("type",lit("antenna_bytes_total"))
      .select($"window.start".as("timestamp"),$"id", $"value", $"type")


  }

  override def DevicesCountBytesEmail(dataFrame: DataFrame): DataFrame = {

    dataFrame
      .select($"device.timestamp",$"email".alias("id"),$"bytes" )
      .groupBy($"id",window($"timestamp","1 hour"))
      .agg(
        sum("bytes").as("value")

      )
      .withColumn("type",lit("email_bytes_total"))
      .select($"window.start".as("timestamp"),$"id", $"value", $"type")
  }


  override def DevicesCountBytesApp(dataFrame: DataFrame): DataFrame = {

    dataFrame
      .select($"timestamp",$"app".alias("id"),$"bytes" )
      .groupBy($"id",window($"timestamp","1 hour"))
      .agg(
        sum("bytes").as("value")

      )
      .withColumn("type",lit("app_bytes_total"))
      .select($"window.start".as("timestamp"),$"id", $"value", $"type")
  }

  override def DevicesEmailSuperiorCuota(dataFrame: DataFrame): DataFrame = {

    dataFrame
      .select($"timestamp",$"email",$"bytes",$"quota")
      .withWatermark("timestamp","15 seconds")
      .groupBy($"email",$"quota",window($"timestamp","1 hour"))
      .agg(
        sum("bytes").as("usage")

      )
      .filter($"quota" < $"usage")
      .select($"window.start".as("timestamp"),$"email", $"usage",$"quota")


  }


  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath}/historical")
  }


  def main(args: Array[String]):Unit = {

    run(Array("2022-02-26T17:00:00Z", "/Users/elias/Desktop/practica-Bigdata-procesing/PRACTICA/Practica/src/main/resources/", s"jdbc:postgresql://35.223.224.232:5432/postgres", "user_metadata", "bytes_hourly", "user_quota_limit", "postgres","keepcoding"))

  }

}
