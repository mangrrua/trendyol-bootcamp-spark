package com.trendyol.bootcamp.batch

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.Try

object BatchExampleJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark DataFrame Examples")
      .getOrCreate()

    val initialStartTime     = "20210101"
    val yyyyMMddFormatter    = DateTimeFormatter.ofPattern("yyyyMMdd")
    val currentTime          = LocalDateTime.now().truncatedTo(ChronoUnit.DAYS)
    val formattedCurrentDate = currentTime.format(yyyyMMddFormatter)

    // If you want to see all logs, set log level to info
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val viewLastProcessedTimeDS = Try(
      spark.read
        .json("output/batch/*")
        .select(max($"partition_date"))
    ).getOrElse(spark.emptyDataset[String])

    val viewLastProcessedTime = viewLastProcessedTimeDS
      .head(1)
      .headOption
      .getOrElse(initialStartTime)

    val productsSchema = Encoders.product[Product].schema
    val products = spark.read
      .schema(productsSchema)
      .json("data/batch/products")
      .as[Product]

    val viewSchema = Encoders.product[ProductViewEvent].schema
    val views = spark.read
      .schema(viewSchema)
      .json("data/batch/views/")
      .filter($"partition_date" > viewLastProcessedTime)
      .as[ProductViewEvent]

    val joinedResult = views
      .join(products, views("productId") === products("id"), "inner")
      .drop(products("timestamp"))
      .select($"userId", $"productId", $"channel", $"category")
      .as[EnrichedViewEvent]

    val rankWindow = Window.partitionBy("channel", "category").orderBy("distinctUserCount")

    val reportResult = joinedResult
      .groupByKey(event => (event.channel, event.category))
      .mapGroups {
        case ((channel, category), eventsIterator) =>
          val distinctUserCount = eventsIterator.map(_.userId).toSet.size
          ChannelCategoryView(channel, category, distinctUserCount)
      }
      .withColumn("rank", rank().over(rankWindow))
      .filter($"rank" <= 3)
      .drop("rank")
      .as[ChannelCategoryView]

    reportResult
      .withColumn("partition_date", lit(formattedCurrentDate))
      .repartition(1)
      .write
      .partitionBy("partition_date")
      .mode(SaveMode.Append)
      .json("output/batch")
  }

}

case class ProductViewEvent(userId: Long, productId: Long, channel: String, timestamp: Long)
case class Product(id: Long, category: String, timestamp: Long)
case class EnrichedViewEvent(userId: Long, productId: Long, channel: String, category: String)
case class ChannelCategoryView(channel: String, category: String, distinctUserCount: Int)
