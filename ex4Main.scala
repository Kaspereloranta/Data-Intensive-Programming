package ex4

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{window, column, desc, col}


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
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

import org.apache.spark.ml.regression.LinearRegressionModel

import java.lang.Thread
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

object ex4Main extends App {
  
                        
// Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)
  
	val spark = SparkSession.builder()
                          .appName("assignment")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()                       
  import spark.implicits._

                            
  // Wikipedia defines: Simple Linear Regression
  // In statistics, simple linear regression is a linear regression model with a single explanatory variable.
  // That is, it concerns two-dimensional sample points with one independent variable and one dependent variable
  // (conventionally, the x and y coordinates in a Cartesian coordinate system) and finds a linear function (a non-vertical straight line)
  // that, as accurately as possible, predicts the dependent variable values as a function of the independent variables. The adjective simple
  // refers to the fact that the outcome variable is related to a single predictor.
  
  // You are given an dataRDD of Rows (first element is x and the other y). We are aiming at finding simple linear regression model 
  // for the dataset using MLlib. I.e. find function f so that y ~ f(x)

  val hugeSequenceOfxyData = Seq(Row(0.0, 0.0), Row(0.3, 0.5), Row(0.9, 0.8), Row(1.0, 0.8), Row(2.0, 2.2), Row(2.2, 2.4), Row(3.0, 3.7), Row(4.0, 4.3))
  val dataRDD: RDD[Row] = spark.sparkContext.parallelize(hugeSequenceOfxyData)
  
  //  Task #1:Transform dataRDD to a DataFrame dataDF, with two columns "X" (of type Double) and "label" (of type Double).
  //       (The default dependent variable name is "label" in MLlib)
  
  val schema = new StructType().add(StructField("X", DoubleType, true)).add(StructField("label",DoubleType,true))
  val dataDF: DataFrame = spark.createDataFrame(dataRDD,schema)
  dataDF.show()
  
  // Let's split the data into training and testing datasets
  val trainTest = dataDF.randomSplit(Array(0.7, 0.3))
  val trainingDF = trainTest(0)
  trainingDF.show
  
  // Task #2: Create a VectorAssembler for mapping input column "X" to "feature" column and apply it to trainingDF
  import org.apache.spark.ml.feature.VectorAssembler
  val vectorAssembler = new VectorAssembler().setInputCols(Array("X")).setOutputCol("feature")

  val assembledTrainingDF = vectorAssembler.transform(trainingDF)
  assembledTrainingDF.show

  // Task #3: Create a LinearRegression object and fit the assembledTrainingDF to get a LinearRegressionModel object
  import org.apache.spark.ml.regression.LinearRegression
  val lr: LinearRegression = new LinearRegression().setFeaturesCol("feature")
  println(lr.explainParams())
  val lrModel: LinearRegressionModel = lr.fit(assembledTrainingDF)
  lrModel.summary.predictions.show
 
  // Task #4: Apply the model to the whole dataDF
  val allPredictions: DataFrame = dataDF
  //allPredictions.show
  val assembledDF = vectorAssembler.transform(dataDF)
  //assembledDF.show
  val lr2: LinearRegression = new LinearRegression().setFeaturesCol("feature")
  val lrModel2: LinearRegressionModel = lr2.fit(assembledDF)
  lrModel2.summary.predictions.show
 
  // Task #5: Use the LinearRegressionModel to predict y for values [-0.5, 3.14, 7.0]
  val data = Seq(-0.5,3.14,7.0)
  val rdd = spark.sparkContext.parallelize(data)
  val df = rdd.toDF("X")
  val assembled = vectorAssembler.transform(df)
  val assembled2 = lrModel2.transform(assembled)  
  assembled2.show

}