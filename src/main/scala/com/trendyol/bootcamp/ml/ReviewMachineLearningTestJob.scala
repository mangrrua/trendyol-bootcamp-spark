package com.trendyol.bootcamp.ml

import com.trendyol.bootcamp.streaming.CustomerReview
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{Encoders, SparkSession}

object ReviewMachineLearningTestJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Test Review Dataset ML Model Job")
      .getOrCreate()

    import spark.implicits._

    val modelInputPath  = "models/reviews-ml"
    val testingDataPath = "data/ml/review-dataset-test.json"

    val savedModel = PipelineModel.load(modelInputPath)

    val schema = Encoders.product[CustomerReview].schema
    val data = spark.read
      .schema(schema)
      .json(testingDataPath)
      .as[CustomerReview]

    val predictions = savedModel.transform(data)

    predictions.show(false)
  }

}
