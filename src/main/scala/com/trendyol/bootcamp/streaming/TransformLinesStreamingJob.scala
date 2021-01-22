package com.trendyol.bootcamp.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object TransformLinesStreamingJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("sss - reformat input data job")
      .getOrCreate()

    import spark.implicits._

    val inputLines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val transformedLines = inputLines.as[String]
      .filter(_ != "")
      .map(convertToLineDetail)

    val query = transformedLines.writeStream
      .outputMode("update")
      .format("console")
      .trigger(Trigger.ProcessingTime(5000))
      .start()

    query.awaitTermination()
  }

  def convertToLineDetail(line: String): LineDetail = {
    val totalWordCount = line.split(" ").length
    LineDetail(line, totalWordCount)
  }

}

case class LineDetail(inputLine: String, totalWordCount: Int, receivedTimestamp: Long = System.currentTimeMillis())