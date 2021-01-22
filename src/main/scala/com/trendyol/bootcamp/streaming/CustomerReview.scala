package com.trendyol.bootcamp.streaming

case class CustomerReview(
    customerId: String,
    productId: String,
    reviewId: String,
    reviewDate: Long,
    reviewBody: String) {

  def isValid: Boolean = reviewBody != ""
}

case class CustomerReviewPrediction(
    customerId: String,
    productId: String,
    reviewId: String,
    reviewDate: Long,
    prediction: Int) {

  def toCustomerReviewPredictionResult: CustomerReviewPredictionResult = {
    val predictionAsStr = prediction match {
      case 0 => "negative"
      case 1 => "positive"
      case _ => throw new IllegalStateException(s"Bad prediction: $prediction")
    }

    CustomerReviewPredictionResult(customerId, productId, reviewId, reviewDate, predictionAsStr)
  }
}

case class CustomerReviewPredictionResult(
    customerId: String,
    productId: String,
    reviewId: String,
    reviewDate: Long,
    prediction: String
)
