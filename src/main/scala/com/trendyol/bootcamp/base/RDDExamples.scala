package com.trendyol.bootcamp.base

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD examples
 */
object RDDExamples {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Spark RDD Examples")

    val sc = new SparkContext(sparkConf)

    // If you want to see all logs, set log level to info
    sc.setLogLevel("ERROR")

    /**
      * Create RDD from array
      */
    val numbersRDD = sc.parallelize(Array(1, 2, 3, 4, 5))

    /**
      * Basic operations on RDD
      */
    numbersRDD.foreach(println) // print for each line
    numbersRDD.foreachPartition(it => it.size) // do some operation on each partition
    println(s"RDD first element: ${numbersRDD.first()}")
    println(s"Count: ${numbersRDD.count()}")

    val sum = numbersRDD.reduce((acc, next) => acc + next)
    println(s"Sum: $sum")

    println("First n rows from RDD")
    val firstNRows = numbersRDD.take(3)
    firstNRows.foreach(println)

    /**
      * Create RDD from txt file
      */
    println("Lines:")
    val linesRDD = sc.textFile("data/examples/lines_example.txt")
    linesRDD.foreach(println)

    /**
      * Map, flatMap, filter operations on RDD
      */
    println("Line lengths:")
    // calculate line lengths
    linesRDD
      .map(line => line.length)
      .take(5)
      .foreach(println)

    // split line to words
    val nonEmptyWords = linesRDD
      .flatMap(line => line.split(" "))
      .filter(word => word.nonEmpty)

    println("Nonempty words:")
    nonEmptyWords.take(5).foreach(println)

    /**
      * RDD Key-Value Pairs
      * Key value pairs is used when you want to aggregate, group etc data
      */
    // convert words to word, count tuple (key = word, value = 1)
    val wordWithCounts = nonEmptyWords.map(word => (word, 1))

    // find word count using groupByKey
    val wordCountGroupByKeyResult = wordWithCounts
      .groupByKey()
      .mapValues(values => values.size)

    wordCountGroupByKeyResult
      .take(5)
      .foreach(println)
    println(s"GroupByKey Result Query plan: \n ${wordCountGroupByKeyResult.toDebugString}")

    // find word count using reduceByKey
    val reducedWordCounts = wordWithCounts.reduceByKey((first, second) => first + second)
    reducedWordCounts
      .collect()
      .foreach(println)
    println(s"ReduceByKey Result Query plan: \n ${wordCountGroupByKeyResult.toDebugString}")

    /**
      * Sort RDD by key or value
      */
    // Sort words by counts (sort by value)
    reducedWordCounts
      .sortBy(_._2) // sortBy(_._2, false) // for desc order
      .collect()
      .foreach(println)

    // Sort words alphabetically (sort by key)
    reducedWordCounts
      .sortByKey(false) // sortByKey() // for asc order
      .collect()
      .foreach(println)

    /**
      * Cache RDD
      */
    val cachedNonEmptyWords = nonEmptyWords.persist(StorageLevel.MEMORY_ONLY) // MEMORY_AND_DISK etc
    // cachedNonEmptyWords.unpersist()

    /**
      * Broadcast variables
      * Use broadcast variables when you want to send any data to executors
      */
    val broadcastArray = sc.broadcast("An example data will be sent to executors")
    val broadcastRes   = broadcastArray.value
    println(s"Broadcast val: $broadcastRes")

    /**
      * Repartition RDD
      * repartition: increase or decrease partition
      * coalesce: decrease partition
      */
    // print current partition count
    println(s"Current partition count: ${cachedNonEmptyWords.getNumPartitions}")

    // repartition
    val repartitionedRDD = cachedNonEmptyWords.repartition(4)
    println(s"Partition count after repartition: ${repartitionedRDD.getNumPartitions}")

    val coalescedRDD = repartitionedRDD.coalesce(2) // shuffle = true
    println(s"Partition count after coalesce: ${coalescedRDD.getNumPartitions}")

    /**
      * Join RDDs
      * RDDs should be key-value format for joining
      */
    // create user key-value pair
    val userKeyValueRDD = sc
      .parallelize(
        Array(
          User(2, "lanadelrey"),
          User(1, "sia")
        )
      )
      .map(user => (user.userId, user))

    // create order key-value pair
    val ordersKeyValueRDD = sc
      .parallelize(
        Array(
          Order(1, 1, "lana's phone"),
          Order(2, 2, "sia's phone"),
          Order(3, 2, "tarkan's phone")
        )
      )
      .map(order => (order.userId, order))

    userKeyValueRDD
      .join(ordersKeyValueRDD)
      .foreach(println)

    /**
      * Serialization in Spark
      * Class, method, function etc. is sent to executors from driver.
      * So
      */
    // upperCase words - apply filter with passed function
    println("upper case words:")
    nonEmptyWords
      .filter(MyFunctions.isUpperCase)
      .take(2)
      .foreach(println)

    // reverse words - apply map with passed function
    println("reversed words:")
    nonEmptyWords
      .map(MyFunctions.reverse)
      .take(2)
      .foreach(println)

    // Codes below will throw 'org.apache.spark.SparkException: Task not serializable' exception
    // Because Spark tries the serialize whole class when you want to access class's fields or methods
    val myClass = new MyClass()
    myClass
      .numsToStrings(numbersRDD)
      .take(2)
      .foreach(println)

    myClass
      .addPrefix(nonEmptyWords)
      .take(2)
      .foreach(println)
  }

}

case class User(userId: Long, userName: String)
case class Order(orderId: Long, userId: Long, productName: String)

object MyFunctions {

  def isUpperCase(word: String): Boolean =
    word.toUpperCase == word

  def reverse(word: String): String =
    (for (i <- word.length - 1 to 0 by -1) yield word(i)).mkString
}

class MyClass {
  val prefix = "bootcamp_"

  def numToString(num: Int): String =
    num.toString

  def numsToStrings(rdd: RDD[Int]): RDD[String] =
    rdd.map(numToString)

  def addPrefix(rdd: RDD[String]): RDD[String] =
    rdd.map(str => s"$prefix$str")
}
