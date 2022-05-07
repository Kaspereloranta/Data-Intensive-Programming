package ex3

import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, IntegerType, DoubleType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{sum, min, max, asc, desc, udf}

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession

import com.databricks.spark.xml._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang.Thread
import sys.process._

import org.apache.log4j.Logger
import org.apache.log4j.Level

object ex3Main extends App {

  // Suppress the log messages
  Logger.getLogger("org").setLevel(Level.OFF)

  // https://docs.databricks.com/spark/latest/data-sources/read-csv.html
	val spark = SparkSession.builder()
                          .appName("ex3")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "5")

  
  // Task 1: File "data/sales_data_sample.csv" contains sales data of a retailer.
  //         Study the file and read the data into DataFrame retailerDataFrame.
  val  retailerDataFrame: DataFrame = spark.read.option("inferSchema","true").option("header","true").option("delimiter", ",")
  .csv("data/sales_data_sample.csv")
  retailerDataFrame.createOrReplaceTempView("retailer_data")
  retailerDataFrame.printSchema()
  
  // Task 2: Find the best 10 selling days. That is the days for which QUANTITYORDERED * PRICEEACH
  //         gets the highest values. 
  val best10DaysDF = spark.sql("""
      SELECT ORDERDATE, ROUND(SUM(QUANTITYORDERED*PRICEEACH),2)
      FROM retailer_data
      GROUP BY ORDERDATE
      ORDER BY SUM(QUANTITYORDERED*PRICEEACH) DESC
      LIMIT 10
      """)
  best10DaysDF.show()

    
  // Task 3: In the streaming version of the analysis, the streaming data will be added 
  //         into the directory streamingData. Data is streaming in as files similar to the one
  //         in the directory data.
  //
  //         Change static retailerDataFrame to operate with streaming data.
  //         Hint: Spark cannot infer the schema of streaming data, so you have to give it explicitly. 
  
  val myManualSchema = StructType(Array(StructField("ORDERNUMBER", IntegerType, true),StructField("QUANTITYORDERED", IntegerType, true),
      StructField("PRICEEACH", DoubleType, true),StructField("ORDERLINENUMBER", IntegerType, true),StructField("SALES", DoubleType, true),
      StructField("ORDERDATE", StringType, true),StructField("STATUS", StringType, true),StructField("TR_ID", IntegerType, true),
      StructField("MONTH_ID", IntegerType, true),StructField("YEAR_ID", IntegerType, true),StructField("PRODUCTLINE", StringType, true),
      StructField("MSRP", IntegerType,true),StructField("PRODUCTCODE", StringType,true),StructField("CUSTOMERNAME", StringType,true),
      StructField("PHONE", StringType,true),StructField("ADDRESSLINE1", StringType,true),StructField("ADDRESSLINE2", StringType,true),
      StructField("CITY", StringType,true),StructField("STATE", StringType,true),StructField("POSTALCODE", StringType,true),
      StructField("COUNTRY", StringType,true),StructField("TERRITORY", StringType,true),StructField("CONTACTLASTNAME", StringType,true),
      StructField("CONTACTFIRSTNAME", StringType,true),StructField("DEALSIZE", StringType,true)))
      
  val  retailerStreamingDF: DataFrame =  spark.readStream.option("header","true").option("delimiter", ",").schema(myManualSchema).load("streamingData/*.csv")
  retailerStreamingDF.printSchema()

  // Task 4: The classes that takes a type just like a parameter are known to be Generic 
  //         Classes in Scala. Dataset is an example of a generic class. Actually, DataFrame is
  //         a type alias for Dataset[Row], where Row is given as a type parameter. Declare your 
  //         own case class Sales with two members: year and euros of type integer. The
  //         class must be declared before this object (ex3Main).
  
  //         Then instantiate a Dataset[Sales] and query for the sales on 2019 and 
  //         the year of maximum sales.
  
  case class Sales(year: Int, euros: Int)
  
  import spark.implicits._
  val salesList = List(Sales(2016, 100), Sales(2017, 15), Sales(2018, 1000), Sales(2019, 50))
  val salesDS: Dataset[Sales] = spark.createDataset(salesList)
  salesDS.show() 

  val sales2019: Sales = salesDS.filter($"year"===2019).first()
 
  println(f"Sales for 2019 is ${sales2019.euros}")
  
  val maximumSales: Sales = salesDS.filter($"euros" === salesDS.select(max($"euros")).first()(0)).first()
  
  println(f"Maximum sales: year = ${maximumSales.year}, euros = ${maximumSales.euros}")


  // Extra Task 5: Find the best 10 selling days in the streaming data
                                              
  // val best10DaysDFStreaming = ???


  // Extra Task 6: Test your solution by writing the 10 best selling days to stdout
  //         whenever the DataFrame changes
 
  // ???

  // You can test your solution by uncommenting the following code snippet. The loop
  // adds a new CSV file to the directory streamingData every 5th second. If you rerun the test
  // remove all the CSV files from the directory streamingData.
  
//  val repoFiles = "ls streamingDataRepo" !!
//  
//  for(f <- repoFiles.split("\n")){
//	  val cmd = f"cp streamingDataRepo/${f} streamingData/${f}.csv"
//    val res = cmd !!
//      
//    Thread.sleep(5000)
//  }
  
  
}
