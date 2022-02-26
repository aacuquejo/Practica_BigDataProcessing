package io.keepcoding.spark.exercise.streaming
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, dayofmonth, from_json, hour, lit, month, sum, window, year}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, duration}

object DeviceStreamingJob  extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._
  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()

  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val deviceMessageSchema: StructType = ScalaReflection.schemaFor[DeviceMessage].dataType.asInstanceOf[StructType]
    dataFrame
      .select(from_json(col("value").cast(StringType), deviceMessageSchema).as("json"))
      .select("json.*")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
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


  override def enrichDevicesWithMetadata(deviceDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    deviceDF.as("device")
      .join(
        metadataDF.as("metadata"),
        $"device.id" === $"metadata.id"
      ).drop($"metadata.id")
  }
  override def DevicesCountBytesAntenna(dataFrame: DataFrame): DataFrame =  {

    dataFrame
      .select($"timestamp",$"antenna_id".alias("id"),$"bytes" )
      .withWatermark("timestamp","15 seconds")
      .groupBy($"id",window($"timestamp","5 minutes"))
      .agg(
        sum("bytes").as("value")

      )
      .withColumn("type",lit("antenna_bytes_total"))
      .select($"window.start".as("timestamp"),$"id", $"value", $"type")
  }

  override def DevicesCountBytesUser(dataFrame: DataFrame): DataFrame = {

    dataFrame
      .select($"device.timestamp",$"id",$"bytes" )
      .withWatermark("timestamp","15 seconds")
      .groupBy($"id",window($"timestamp","5 minutes"))
      .agg(
        sum("bytes").as("value")

      )
      .withColumn("type",lit("user_bytes_total"))
      .select($"window.start".as("timestamp"),$"id", $"value", $"type")

  }

  override def DevicesCountBytesApp(dataFrame: DataFrame): DataFrame = {

    dataFrame
      .select($"timestamp",$"app".alias("id"),$"bytes" )
      .withWatermark("timestamp","15 seconds")
      .groupBy($"id",window($"timestamp","5 minutes"))
      .agg(
        sum("bytes").as("value")

      )
      .withColumn("type",lit("app_bytes_total"))
      .select($"window.start".as("timestamp"),$"id", $"value", $"type")

  }


  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String):  Future[Unit] = Future{
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
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
      .start()
      .awaitTermination()
  }
  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )

    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"${storageRootPath}")
      .option("checkpointLocation", s"${storageRootPath}/checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]):Unit = {

   run(Array("34.142.1.69:9092","devices",s"jdbc:postgresql://35.223.224.232:5432/postgres","user_metadata","bytes","postgres","keepcoding","/Users/elias/Desktop/practica-Bigdata-procesing/PRACTICA/Practica/src/main/resources/"))


  }





}
