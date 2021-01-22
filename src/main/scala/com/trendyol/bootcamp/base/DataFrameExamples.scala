package com.trendyol.bootcamp.base

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

/**
  * Spark DataFrame examples
  */
object DataFrameExamples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark DataFrame Examples")
      .getOrCreate()

    // If you want to see all logs, set log level to info
    spark.sparkContext.setLogLevel("ERROR")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    /**
      * Create example users data from sequence
      */
    val users = Seq(
      ("lana", 25, "tr", 10),
      ("del", 30, "en", 5),
      ("rey", 30, "tr", 20)
    ).toDF("name", "age", "country", "salary")
    users.show()

    /**
      * Select some column(s) from dataframe and create new one.
      * Same with ''select * or select col1, col2..'' in SQL
      */
    users.select("name").show()
    // You can also specify expression in select method like 'age + 1'
    // That means spark add 1 to age
    users.select($"name", $"age" + 1).show()

    /**
      * Filter dataframe based on given condition(s) using column methods or expressions
      */
    users.filter($"name" === "lana").show()
    // Filter using expression
    users.filter("name == 'del'").show()

    /**
      * Do some operation on a column of dataframe
      * This is similar with 'map' in RDD, Dataset
      */
    // Create new column from existing column
    users.withColumn("newColumn", $"age").show()
    // Create new column from new value
    users.withColumn("agePlusOneCol", $"age" + 1).show()
    // Overwrite age column with new value
    users.withColumn("age", $"age" + 15).show()
    // Rename column name (select name as newColName in SQL)
    users.withColumnRenamed("name", "newColName").show()

    /**
      * Map operation on a row
      * DataFrame is not type safe, and it represented Row format in spark
      * DataFrame or Dataset[Row]
      */
    users.map(row => row.getAs[String](0)).show()
    users.map(row => row.getAs[String]("name")).show()
    users.map(row => row.getInt(1)).show()

    /**
      * Statistical or mathematical operations on DataFrame
      */
    users.describe("age").show(false)
    val ageSalaryCov = users.stat.cov("age", "salary")
    println(s"Age-Salary Cov: $ageSalaryCov")
    val ageSalaryCorr = users.stat.corr("age", "salary")
    println(s"Age-Salary Corr: $ageSalaryCorr")

    /**
      * Group by operations on DafaFrame
      */
    // Count people by age
    users
      .groupBy("age")
      .count()
      .show()

    // find sum, avg etc
    users
      .groupBy("country")
      .agg(sum("age"), avg("age"), count("*"))
      .show()

    /**
      * DataFrame caching and unpersisting
      * Use cache if you use the same dataframe more than once
      */
    val cachedUserAges = users.cache()
    cachedUserAges.unpersist()

    /**
      * User Defined Functions - Custom functions on DataFrame
      * It's similar with transformations like map/filter etc
      *
      * Note: Spark can not optimize the UDFs
      */
    // plus one to age again using udfs
    def agePlusN(plus: Int): UserDefinedFunction = udf((age: Int) => age + plus)
    val agePlus1Udf                              = agePlusN(1)

    users
      .withColumn("agePlus1UsingUdf", agePlus1Udf($"age"))
      .show()

    /**
      * Read click stream data from json file
      */
    val clickDataSchema = Encoders.product[ClickData].schema
    val clicks = spark.read
      .schema(clickDataSchema)
      .json("data/examples/clickstream_example.json")

    // group by - distinct count on clickstream data
    clicks
      .groupBy($"event")
      .agg(countDistinct($"userId") as "totalUser")
      .show()

    // Find daily 'addToCart' count by user
    val addToCartCounts = clicks
      .filter($"event" === "addToCart")
      .select("userId", "timestamp")
      .withColumn("timestamp", ($"timestamp" / 1000).cast("timestamp")) // timestamp in millisecond format, convert second format
      .withColumn("dailyTrunc", date_trunc("day", $"timestamp")) // convert millis to Timestamp format
      .groupBy($"dailyTrunc", $"userId")
      .agg(count("*") as "totalAddToCart")
      .orderBy($"totalAddToCart".desc)

    addToCartCounts.show(false)
    addToCartCounts.explain()

    // Find the users purchased products which favorited before
    val window = Window.partitionBy($"userId", $"productId").orderBy($"timestamp".asc)
    // Check previous row using 'lag'. if you want to check next row, use 'lead'
    val condition = when(
      $"event" === "addToCart" && lag($"event", 1).over(window) === "favorite",
      true
    ).otherwise(false)
    val validEventTypes = List("addToCart", "favorite")

    clicks
      .filter($"event".isInCollection(validEventTypes))
      .withColumn("boughtViewedProduct", condition)
      .where($"boughtViewedProduct")
      .select("userId", "productId", "timestamp")
      .show(false)

    /**
      * Join DataFrames
      */
    val userDf = Seq(
      (1, "user1"),
      (2, "user2")
    ).toDF("userId", "userName")

    val orderDf = Seq(
      (1, 1, "order1"),
      (2, 1, "order2"),
      (3, 2, "order3")
    ).toDF("orderId", "userId", "userName")

    userDf
      .join(orderDf, userDf("userId") === orderDf("userId"), "inner") // left, right etc.
      .drop(orderDf("userId")) // userId is exist both userDf and orderDf. We want to get userId from 'userDf', so we dropped userId column of orderDf
      .show()

    /**
      * Write dataframe as json, parquet etc
      */
    // write as parquet
    clicks
      .repartition(5) // this will create 5 files in target folder
      .write
      .mode(SaveMode.Append) // If any data exist in target, new data will be added. If not, it creates target folder and add new data
      .parquet("output/examples/clickstream/parquet")

    // write as json with partitioned folders
    clicks
      .repartition($"event") // repartition data in spark
      .write
      .partitionBy("event") // this will create folder for each event type like event=favorite, event=view etc.
      .mode(SaveMode.Overwrite) // Overwrite data in target folder
      .json("output/examples/clickstream/json")
  }

}
