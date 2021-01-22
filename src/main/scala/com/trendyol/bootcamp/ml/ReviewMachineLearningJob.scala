package com.trendyol.bootcamp.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ReviewMachineLearningJob {

  def main(args: Array[String]): Unit = {

    /**
      * Spark ML expects ''features'' and ''label'' columns to train model for classification.
      *
      * For review prediction;
      *  - Input data contains informations about reviews like reviewId, reviewBody etc. and each review have labeled
      *  - To train review data;
      *      * Tokenize ''reviewBody'' to words
      *      * Find term frequencies of words using HashingTF
      *      * Classify input data using logistic regression, naive bayes etc
      *  - Save trained model to file system
      */

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Train Review Dataset ML Model Job")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val modelOutputPath  = "models/reviews-ml"
    val trainingDataPath = "data/ml/review-dataset-train.json"

    val schema = StructType(
      List(
        StructField("customerId", StringType),
        StructField("productId", StringType),
        StructField("reviewId", StringType),
        StructField("reviewDate", LongType),
        StructField("reviewBody", StringType),
        StructField("label", IntegerType)
      )
    )

    val trainingData = spark.read
      .schema(schema)
      .json(trainingDataPath)

    val tokenizer = new Tokenizer()
      .setInputCol("reviewBody")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("features")
      .setNumFeatures(25)
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
    val model    = pipeline.fit(trainingData)

    model.write.overwrite().save(modelOutputPath)
  }
}
