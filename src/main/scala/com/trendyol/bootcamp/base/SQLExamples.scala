package com.trendyol.bootcamp.base

import org.apache.spark.sql.{Encoders, SparkSession}

/**
  * Spark SQL based examples
  */
object SQLExamples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL Examples")
      .getOrCreate()

    // If you want to see all logs, set log level to info
    spark.sparkContext.setLogLevel("ERROR")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    /**
      * Create example data from sequence
      */
    val users = Seq(
      (1, "lana"),
      (2, "del"),
      (3, "rey")
    ).toDF("userId", "userName")
    users.show(10, false)

    /**
      * Register the DataFrame as a SQL temporary view.
      * Temporary view is session scope and will disappear if the session that creates it terminates.
      */
    users.createOrReplaceTempView("temp_users")
    spark.sql("select * from temp_users").show()

    /**
      * Register the DataFrame as a SQL global temporary view.
      * Global view is shared among all sessions.
      */
    users.createGlobalTempView("global_users")
    spark.sql("select * from global_temp.global_users").show()

    /**
      * Create click stream dataset from json file and query with Spark SQL
      */
    val clickDataSchema = Encoders.product[ClickData].schema
    val clicks = spark.read
      .schema(clickDataSchema)
      .json("data/examples/clickstream_example.json")

    // create temp view from clicks data
    clicks.createTempView("temp_click")

    // query 1
    spark
      .sql("select * from temp_click where timestamp between 1609448400000 and 1609458400000 order by timestamp asc")
      .show(false)

    // query 2
    spark
      .sql("select event, count(*) from temp_click group by event")
      .show(false)

    /**
    * If you want to access data stored on Hive, you can access the table from spark sql
    * directly without create temp or global views.
    *
    * For example; your table name in hive is "public.my_table"
    *
    * spark.sql("select * from public.my_table")
    * spark.sql("select event, count(*) from public.my_table group by event"")
    * ...
    */

  }

}
