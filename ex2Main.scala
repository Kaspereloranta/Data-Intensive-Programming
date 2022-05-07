
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ArrayType, StringType, StructField}
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

import org.apache.log4j.Logger
import org.apache.log4j.Level

object main extends App {

  // Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)

	val spark = SparkSession.builder()
                          .appName("ex2")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "5")

  // Task 1: File "data/rdu-weather-history.csv" contains weather data in csv format. 
  //         Study the file and read the data into DataFrame weatherDataFrame.
  //         Let Spark infer the schema. Study the schema.
  val weatherDataFrame: DataFrame = spark.read.option("inferSchema","true").option("header","true").option("delimiter", ";")
  .csv("data/rdu-weather-history.csv")
                                         
  // Study the schema of the DataFrame:                                       
  weatherDataFrame.printSchema()
  
  
  // Task 2: print three first elements of the data frame to stdout
  val weatherSample: Array[Row] = weatherDataFrame.take(3)
  weatherSample.foreach(println) 
    
  
  
  // Task 3: Find min and max temperatures from the whole DataFrame
  weatherDataFrame.createOrReplaceTempView("weather_data")
  // only to find out the amount of rows in data
  val count = spark.sql("""
    SELECT COUNT(temperaturemin)
    FROM weather_data
    """)
  count.show()
                                       
  val minTempArray: Array[Row] = weatherDataFrame.select(min("temperaturemin")).take(4246) // this number was found above
  val maxTempArray: Array[Row] = weatherDataFrame.select(max("temperaturemax")).take(4246)
  
  print("Minimum temperature is", minTempArray(0))
  print("\n")
  print("Maximum temperature is", maxTempArray(0))
  print("\n")
  
  // Task 4: Add a new column "year" to the weatherDataFrame. 
  // The type of the column is integer and value is calculated from column "date".
  // You can use function year from org.apache.spark.sql.functions
  // See documentation https://spark.apache.org/docs/2.3.0/api/sql/index.html#year
  
  import org.apache.spark.sql.functions.year
  val weatherDataFrameWithYear = weatherDataFrame.withColumn("year",year(col("date")))
  weatherDataFrameWithYear.createOrReplaceTempView("weather_data_with_year")
  weatherDataFrameWithYear.printSchema()
  
  // just to see whether the new column was added accordingly 
  val dy = spark.sql("""
    SELECT date, year
    FROM weather_data_with_year
    """)
  dy.show()
  
  // Task 5: Find min and max for each year
  val aggregatedDF: DataFrame = spark.sql("""
    SELECT MIN(temperaturemin) as min, MAX(temperaturemax) as max, YEAR
    FROM weather_data_with_year
    GROUP BY YEAR 
    ORDER BY YEAR
    """)
  
  aggregatedDF.printSchema()
  aggregatedDF.collect().foreach(println) 
  
  
}