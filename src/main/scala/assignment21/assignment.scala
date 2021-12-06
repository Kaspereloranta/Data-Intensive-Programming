package assignment21

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

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.ml.clustering.{KMeans, KMeansSummary}


import java.io.{PrintWriter, File}


//import java.lang.Thread
import sys.process._


import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.immutable.Range

object assignment  {
  // Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)
                       
  
  val spark = SparkSession.builder()
                          .appName("assignment")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()
                          
  val mySchema1 = StructType(Array(
      StructField("a",DoubleType,true),
      StructField("b",DoubleType,true),
      StructField("LABEL",StringType,true)))
      
  val mySchema2 = StructType(Array(
      StructField("a",DoubleType,true),
      StructField("b",DoubleType,true),
      StructField("c",DoubleType,true),
      StructField("LABEL",StringType,true)))    
      
                     
  val dataK5D2 =  spark.read.option("header","true")
                      .option("delimiter",",")
                      .schema(mySchema1)
                      .csv("data/dataK5D2.csv")
                      .cache()

  val dataK5D3 =  spark.read.option("header","true")
                      .option("delimiter",",")
                      .schema(mySchema2)
                      .csv("data/dataK5D3.csv")
                      .cache()
  
  val indexer = new StringIndexer().setInputCol("LABEL").setOutputCol("mappedLABEL")
  val dataK5D3WithLabels = indexer.fit(dataK5D2).transform(dataK5D2)
  dataK5D3WithLabels.show()
  
  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
    val kdf = df.select("a","b")
      
    val vectorAssembler = new VectorAssembler().setInputCols(Array("a","b"))
                                               .setOutputCol("features")
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(kdf)
    val transformedData = pipeLine.transform(kdf)
        
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val scalerModel = scaler.fit(transformedData)
    val scaledData = scalerModel.transform(transformedData)
    
    val kmeans = new KMeans().setK(k).setSeed(1L).setFeaturesCol("scaledFeatures")
    val model = kmeans.fit(scaledData)
    val clusterPairs = model.clusterCenters.map(v => (v(0),v(1)))
    println("\n K-means summary for 2-dim data: \n")
    model.summary.predictions.show()
    println("2-dim data clusters: \n")
    clusterPairs.foreach(println)
    println("\n")
    return clusterPairs
  }

  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {
    val kdf = df.select("a","b","c")
      
    val vectorAssembler = new VectorAssembler().setInputCols(Array("a","b","c"))
                                               .setOutputCol("features")
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(kdf)
    val transformedData = pipeLine.transform(kdf)
        
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val scalerModel = scaler.fit(transformedData)
    val scaledData = scalerModel.transform(transformedData)
    
    val kmeans = new KMeans().setK(k).setSeed(1L).setFeaturesCol("scaledFeatures")
    val model = kmeans.fit(scaledData)
    val clusterTuples = model.clusterCenters.map(v => (v(0),v(1),v(2)))
    println("\n K-means summary for 3-dim data: \n")
    model.summary.predictions.show()
    println("3-dim data clusters: \n")
    clusterTuples.foreach(println)
    println("\n")
    return clusterTuples
  }

  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    
    val mappedDf = df.select("a","b","mappedLABEL")
    val vectorAssembler = new VectorAssembler().setInputCols(Array("a","b","mappedLABEL"))
                                               .setOutputCol("features")
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(mappedDf)
    val transformedData = pipeLine.transform(mappedDf)
        
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val scalerModel = scaler.fit(transformedData)
    val scaledData = scalerModel.transform(transformedData)
    val kmeans = new KMeans().setK(k).setSeed(1L).setFeaturesCol("scaledFeatures")
    val model = kmeans.fit(scaledData) 
    
    //val sc = spark.sparkContext
    val clusterTuples = model.clusterCenters //.map(v => (v(0),v(1),v(2)))
    clusterTuples.foreach(println)    
    //val mat: RowMatrix = new RowMatrix(clusters)
    
    val mostFatalClusters: Array[(Double,Double)] = new Array[(Double, Double)](2);

    
    // ALUStAVA TOTEUTUS, TOIMII OIKEIN KOSKA DATA SUOTUISAA
    for(i <- 0 to (clusterTuples.length-1)){
      if(clusterTuples(i)(2) == 1.0){
        mostFatalClusters :+ (clusterTuples(i)(0),clusterTuples(i)(1))
      }
    }
    return mostFatalClusters
  }

  // Parameter low is the lowest k and high is the highest one.
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
    ???
  }
     
  
    
}


