package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class DeviceMessage(timestamp: Timestamp, id: String, antenna_id :String, bytes:  Long, app: String)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readDeviceMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichDevicesWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def DevicesCountBytesAntenna(dataFrame: DataFrame): DataFrame
  def DevicesCountBytesUser(dataFrame: DataFrame): DataFrame
  def DevicesCountBytesApp(dataFrame: DataFrame): DataFrame


  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    //Recuperar y almacenar datos origen
    val kafkaDF = readFromKafka(kafkaServer, topic)
    val deviceDF = parserJsonData(kafkaDF)
    val metadataDF = readDeviceMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val deviceMetadataDF = enrichDevicesWithMetadata(deviceDF, metadataDF)//join
    val storageFuture = writeToStorage(deviceDF, storagePath)

    //Almacenar metricas
    val aggByBytesAntennaDF = DevicesCountBytesAntenna(deviceMetadataDF)
    val aggFuture1 = writeToJdbc(aggByBytesAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    val aggByBytesUserDF = DevicesCountBytesUser(deviceMetadataDF)
    val aggFuture2 = writeToJdbc(aggByBytesUserDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    val aggByBytesAppDF = DevicesCountBytesApp(deviceMetadataDF)
    val aggFuture3 = writeToJdbc(aggByBytesAppDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)



    Await.result(Future.sequence(Seq(aggFuture1,aggFuture2,aggFuture3, storageFuture)), Duration.Inf)

    spark.close()
  }

}
