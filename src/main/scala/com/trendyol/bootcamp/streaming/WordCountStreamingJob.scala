package com.trendyol.bootcamp.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object WordCountStreamingJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("sss - reformat input data job")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val inputLines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

//    val transformedLines = inputLines
//      .as[String]
//      .filter(_ != "")
//      .flatMap(_.split(" "))
//      .groupBy($"value")
//      .count()

    val transformedLines = inputLines
      .as[String]
      .filter(_ != "")
      .flatMap(_.split(" "))
      .map(word => WordWithCount(word, 1))
      .groupByKey(_.word)
      .reduceGroups { (first, second) =>
        val updatedCount = first.count + second.count
        WordWithCount(first.word, updatedCount)
      }
      .map { case (_, value) => value }

    val query = transformedLines.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }

}

case class WordWithCount(word: String, count: Long)
