package com.trendyol.bootcamp.base

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.expressions.scalalang.typed

/**
  * Spark Dataset examples
  */
object DatasetExamples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Dataset Examples")
      .getOrCreate()

    // If you want to see all logs, set log level to info
    spark.sparkContext.setLogLevel("ERROR")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    /**
      * Create dataset from sequence
      */
    val users = Seq(
      UserDetail("lana", 25),
      UserDetail("del", 25),
      UserDetail("rey", 27)
    ).toDS()
    users.show(false)

    /**
      * DataFrame, RDD, Dataset conversions
      */
    val usersDF  = users.toDF()
    val usersRDD = users.rdd

    val ds  = usersDF.as[UserDetail] // df to ds
    val ds1 = usersRDD.toDS()  // because rdd type safe

    /**
      * Map, filter etc. operations on Dataset
      */
    users.filter(_.age == 25).show()
    users.map(_.name).show()
    users
      .map { user =>
        val updatedAge = user.age + 1
        UserDetail(user.name, updatedAge)
      }
      .show()

    /**
      * Group by operations on Dataset
      */
    users
      .groupByKey(_.age)
      .count()
      .show()

    users
      .groupByKey(_.age)
      .agg(typed.count[UserDetail](_.name))
      .show()

    /**
      * Create dataset from json file
      */
    val clickDataSchema = Encoders.product[ClickData].schema
    val clicks = spark.read
      .schema(clickDataSchema)
      .json("data/examples/clickstream_example.json")
      .as[ClickData]
    // .filter($"timestamp" >= 1609448400000L) // to improve read performance (filter push down)
    // .select("event") // to improve read performance (column push down)

    // Calculate total counts for each event type which has timestamp greater than 1609448400000L
    clicks
      .filter(_.timestamp >= 1609448400000L)
      .map(ue => EventWithCount(ue.event, 1L))
      .groupByKey(_.event)
      .reduceGroups { (first, second) =>
        val updatedCount = first.count + second.count
        EventWithCount(first.event, updatedCount)
      }
      .map { case (_, eventCount) => eventCount }
      .show()

    /**
      * Union Operation
      */
    val newClicks = Seq(
      ClickData(1001L, 1000L, "view", System.currentTimeMillis())
    ).toDS()

    val unionedData = clicks.union(newClicks)
    unionedData.filter(_.userId == 1001L).show()

    /**
     * Join Datasets
     */
    val products = Seq(
      Product(1, "p1"),
      Product(2, "p2")
    ).toDS()

    val favorites = Seq(
      Favorite(1, 1, 1L),
      Favorite(2, 1, 1L),
      Favorite(3, 2, 1L)
    ).toDS()

    products
      .joinWith(favorites, products("id") === favorites("productId"), "inner")
      .map { case (product, favorite) =>
        FavoriteResult(favorite.id, product.id, product.name, favorite.timestamp)
      }
      .show()
  }

}

case class UserDetail(name: String, age: Int)
case class EventWithCount(event: String, count: Long)

case class Product(id: Long, name: String)
case class Favorite(id: Long, productId: Long, timestamp: Long)
case class FavoriteResult(favId: Long, productId: Long, productName: String, timestamp: Long)
