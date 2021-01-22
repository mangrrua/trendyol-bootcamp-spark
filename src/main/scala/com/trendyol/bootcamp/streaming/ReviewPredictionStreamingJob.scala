package com.trendyol.bootcamp.streaming

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Encoders, SparkSession}

object ReviewPredictionStreamingJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Review Prediction Streaming Job")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Load saved machine learning model
    val modelInputPath = "models/reviews-ml"
    val reviewMLModel  = PipelineModel.load(modelInputPath)

    // Read reviews from kafka topic
    val reviews = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "reviews")
      .load()
      .select($"value".cast(StringType))

    // Extract json to given model using schema and find valid reviews
    val reviewsSchema = Encoders.product[CustomerReview].schema
    val validReviews = reviews
      .select(from_json($"value", reviewsSchema).as("json_data"))
      .select("json_data.*")
      .as[CustomerReview]
      .filter(_.isValid)

    // Find prediction results of input micro-batch and convert result to output format
    val predictedReviews = reviewMLModel
      .transform(validReviews)
      .select($"customerId", $"productId", $"reviewId", $"reviewDate", $"prediction".cast(IntegerType))
      .as[CustomerReviewPrediction]
      .map(_.toCustomerReviewPredictionResult)

    // write prediction results to console every 2 seconds
    val query = predictedReviews.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2000))
      .start()

    query.awaitTermination()
  }

}
