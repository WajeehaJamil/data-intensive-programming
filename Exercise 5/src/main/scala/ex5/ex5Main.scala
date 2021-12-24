package ex5

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{window, column, desc, col}


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, IntegerType, DoubleType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{count, sum, min, max, asc, desc, udf, to_date, avg}
import org.apache.spark.sql.functions.unix_timestamp

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession

import com.databricks.spark.xml._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


import java.lang.Thread
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

object ex5Main extends App {
  
                        
// Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)
  
	val spark = SparkSession.builder()
                          .appName("ex5")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()
  val sc = spark.sparkContext
  
  // There are three scientific articles in the directory src/main/resources/articles/
  // The call sc.textFile(...) returns an RDD consisting of the lines of the articles:
  val articlesRdd: RDD[String] = sc.textFile("data/aaltonen.txt")
  
  
  // Task #1: How do you get the first 10 lines as an Array
  val lines10 = articlesRdd.take(10)
  lines10.foreach(println)
  
  // Task #2: Compute how many lines there are in the articles
  val nbrOfLines = articlesRdd.flatMap(_.split("\r\n")).map(word => (word, 1)).count()
  println(f"#lines = ${nbrOfLines}%6s")
  

  // Task #3: What about the number of words
  val words = articlesRdd.flatMap(_.split(" ")).map(word => (word, 1)).count()
  println(f"#words = ${words}%6s")
  
  // Task #4: What is the number of non-white space chars?
  val chars = articlesRdd.flatMap(line => line.toCharArray.filter(_ != ' ')).count()

  println(f"#chars = ${chars}%6s")
  
  // Task #5: How many times the word 'DisCo' appears in the corpus?
  val disco = articlesRdd.flatMap(_.split(" ")).filter(_=="DisCo").count()
  println(f"#disco = ${disco}%6s")
  
  // Task #6: How do you "remove" the lines having only word "DisCo". Can you do it without filter-function? 
  val wordToRemove="DisCo"
  val rddToRemove= sc.parallelize(List(wordToRemove))
  val noDisCoLines = articlesRdd.subtract(rddToRemove)
  println(f"#subtract = ${noDisCoLines.count}%6s")

  

  
  
  // Pretend that 'nums' is a huge rdd of integers.
  val nums: RDD[Int] = sc.parallelize(List(2,3,4,5,6,7,8,9,10))
  
  // You are given a factorization function:
  def factorization(number: Int, list: List[Int] = List()): List[Int] = {
    for(n <- 2 to number if (number % n == 0)) {
      return factorization(number / n, list :+ n)
    }
    list
  }                                               //> primes: (number: Int, list: List[Int])List[Int]
 
  // Task #7: Compute an rdd containing all factors of all integers in 'nums'
  val allPrimes = nums.map(num =>(factorization(num)))
  
  // Task #8: Print all the values in allPrimes
  
  allPrimes.foreach(println)
  
  // Bonus task:
  // Here is the code snippet from the slides. Explain how it works:
  val rdd = sc.textFile("lyrics/*.txt")

  val count = rdd.flatMap(line => line.split(" ")) // split the text into words using whit-space character as a splitter
                 .map(word => (word, 1)) // create a tuple for each word
                 .reduceByKey((v1, v2) => v1 + v2) // reduce the tuples by grouping the same words in one tuple and count the number of occurences
                 
  count.collect.foreach(println) 
  
  // This code snippet calculates each word count in the text file.
  
}
