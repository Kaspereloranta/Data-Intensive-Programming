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
  val articlesRdd: RDD[String] = sc.textFile("data/*")
  
  // Task #1: How do you get the first 10 lines as an Array
  val lines10 = articlesRdd.take(10)
  print("\nTask1\n")
  lines10.foreach(println)
  
  // Task #2: Compute how many lines there are in the articles
  val nbrOfLines = articlesRdd.count()
  print("\n Task2 \n")

  println(f"#lines = ${nbrOfLines}%6s")
  
  // Task #3: What about the number of words
  val splitwords = articlesRdd.flatMap(line => line.split(" ")).map(word => 1)
  splitwords.collect
  println("\n Task3 \n")
  println(f"#words = ${splitwords.count()}%6s")
  
  // Task #4: What is the number of non-white space chars?
  val w = articlesRdd.flatMap(line => line.split(" "))
  val chars = w.map(word => word.toCharArray.length())
  val amountofchars = chars.sum
  println("\n Task4 \n")
  println(f"#chars = ${amountofchars}%6s")
  
  // Task #5: How many times the word 'DisCo' appears in the corpus?
  val disco = articlesRdd.flatMap(line => line.split(" ")).filter(_.contains("DisCo")).count()
  println("\n Task5 \n")
  println(f"#disco = ${disco}%6s")
  
  // Task #6: How do you "remove" the lines having only word "DisCo". Can you do it without filter-function? 
 // val noDisCoLines = sc.parallelize(articlesRdd.
 // println(f"#subtract = ${noDisCoLines.count}%6s")

  /*
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
  val allPrimes = nums.???
  
  // Task #8: Print all the values in allPrimes
  ???
*/  

  // Bonus task:
  // Here is the code snippet from the slides. Explain how it works:
 // val rdd = sc.textFile("lyrics/*.txt")

 // val count = rdd.flatMap(line => line.split(" "))
     //            .map(word => (word, 1))
   //              .reduceByKey((v1, v2) => v1 + v2)
                 
//  count.collect.foreach(println) 

}
