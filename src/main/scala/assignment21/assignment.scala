/* Data-Intensive Programming, fall 2021
 * Course's programming assignment 
 * Kasper Eloranta, H274212, kasper.eloranta@tuni.fi
 * Tasks that have been implemented for this assignment:
 * 			- All basic tasks 1-4.
 * 			- In addition bonus tasks 1-3 and 5.
 */

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
import org.apache.spark.sql.functions.{count, sum, min, max, asc, desc, udf, to_date, avg, abs}

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
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.graphx

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
  
  // Decided to keep the amount of shuffle partitions as 
  // the default (200) because assignment pdf states 
  // that the implementation should be written in a way
  // that amount of data would be big although this particular
  // dataset is not actual that big. If needed, amount of shuffle
  // partitions can be quickly adjusted here. Also, amount of 
  // seemed not to have a huge impact on the running time of the program
  // (I tested it with values like 1-5,10,20,50,200,500).
  spark.conf.set("spark.sql.shuffle.partitions","200")
                          
  // BT's 2 & 3: Decided to define own schemas to make the program
  // more efficient and to handle dirty data. By defining own schemas
  // we avoid the problem of Spark infering the schema somehow wrongly 
  // if the dataset included dirty data. 
  val mySchema1 = StructType(Array(
      StructField("a",DoubleType,true),
      StructField("b",DoubleType,true),
      StructField("LABEL",StringType,true)))
      
  val mySchema2 = StructType(Array(
      StructField("a",DoubleType,true),
      StructField("b",DoubleType,true),
      StructField("c",DoubleType,true),
      StructField("LABEL",StringType,true)))    
      
  /*
  	Bonus task 3: Let's assume that data with negative values is somehow measured wrongly 
  	is therefore dirty data. Let's also assume that the measurement error has only caused 
  	the negative sign of the datapoints, so therefore, the dirty data can be cleaned by  
  	modifying the given data in a way that every data point will be replaced with its  
  	absolute value. This is done below, first we read the original data from csv-files,
  	and after that we utilize spark.sql to generate new dataframes with absolute data point
  	values. The data's cleanity will be tested with Dirty data set. (check DIP21TestSuite.scala)
  */
      
  val dataK5D2dirty =  spark.read.option("header","true")
                      .option("delimiter",",")
                      .schema(mySchema1)
                      .csv("data/dataK5D2.csv")
                      .cache()

  val dataK5D3dirty =  spark.read.option("header","true")
                      .option("delimiter",",")
                      .schema(mySchema2)
                      .csv("data/dataK5D3.csv")
                      .cache()
  
  dataK5D2dirty.createOrReplaceTempView("dirtydata2dim")
  dataK5D3dirty.createOrReplaceTempView("dirtydata3dim")
  
  // Dataframe for Task #1
  val dataK5D2 = spark.sql("""
    SELECT abs(a) as a, abs(b) as b, LABEL
    FROM dirtydata2dim
    """)
  
  // Dataframe for Task #2
  val dataK5D3 = spark.sql("""
    SELECT abs(a) as a, abs(b) as b, abs(c) as c, LABEL
    FROM dirtydata3dim
    """) 
  
  // Dataframe for Task #3
  val indexer = new StringIndexer().setInputCol("LABEL").setOutputCol("mappedLABEL")
  val dataK5D3WithLabels = indexer.fit(dataK5D2).transform(dataK5D2)
  
  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
    val kdf = df.select("a","b")     
    val vectorAssembler = new VectorAssembler().setInputCols(Array("a","b"))
                                               .setOutputCol("features")                                               
    // Bonus Task #4                                           
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(kdf)
    val transformedData = pipeLine.transform(kdf)
    
    // Scaling data
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val scalerModel = scaler.fit(transformedData)
    val scaledData = scalerModel.transform(transformedData)
    
    // K-means model and cluster centers
    val kmeans = new KMeans().setK(k).setSeed(1L).setFeaturesCol("scaledFeatures")
    val model = kmeans.fit(scaledData)
    val clusterPairs = model.clusterCenters.map(v => (v(0),v(1)))
    
    // Results
    println("\n Task #1: 2-dim K-means data clusters: \n")
    clusterPairs.foreach(println)
    return clusterPairs
  }

  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {
    val kdf = df.select("a","b","c")      
    val vectorAssembler = new VectorAssembler().setInputCols(Array("a","b","c"))
                                               .setOutputCol("features")                                          
    // Bonus Task #4                                           
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(kdf)
    val transformedData = pipeLine.transform(kdf)
    
    // Scaling data
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val scalerModel = scaler.fit(transformedData)
    val scaledData = scalerModel.transform(transformedData)
    
    // K-means model and cluster centers
    val kmeans = new KMeans().setK(k).setSeed(1L).setFeaturesCol("scaledFeatures")
    val model = kmeans.fit(scaledData)
    val clusterTuples = model.clusterCenters.map(v => (v(0),v(1),v(2)))
    
    // Results
    println("\n Task #2: 3-dim K-means data clusters: \n")
    clusterTuples.foreach(println)
    return clusterTuples
  }

  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {     
    val mappedDf = df.select("a","b","mappedLABEL")
    val vectorAssembler = new VectorAssembler().setInputCols(Array("a","b","mappedLABEL"))
                                               .setOutputCol("features")                                               
    // Bonus Task #4                                           
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(mappedDf)
    val transformedData = pipeLine.transform(mappedDf)
    
    // Scaling data
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val scalerModel = scaler.fit(transformedData)
    val scaledData = scalerModel.transform(transformedData)
    
    // K-means model and cluster centers
    val kmeans = new KMeans().setK(k).setSeed(1L).setFeaturesCol("scaledFeatures")
    val model = kmeans.fit(scaledData) 
    val clusterTuples = model.clusterCenters
    val clusterPairs = clusterTuples.map(v => (v(0),v(1)))
    
    println("\n Task #3: Cluster centers of originally 2-dim data with LABEL column mapped to numeric scale:\n")    
    clusterTuples.foreach(println)
   
    // To find out two most fatal clusters
    val cluster_ind = model.transform(scaledData)
    cluster_ind.createOrReplaceTempView("fatalitydata")
    val fatalClusters = spark.sql("""
      SELECT SUM(mappedLABEL) as Fataliness, prediction
      FROM fatalitydata
      GROUP BY prediction
      ORDER BY Fataliness DESC
      LIMIT 2
      """)
    val fatalIndexes = fatalClusters.select("prediction").collect()
    val mostFatalClusters: Array[(Double,Double)] = new Array[(Double, Double)](2);
    mostFatalClusters(0) = clusterPairs(fatalIndexes(0).getInt(0))
    mostFatalClusters(1) = clusterPairs(fatalIndexes(1).getInt(0))
    
    println("\nTask #3: The two most fatal clusters: \n")    
    mostFatalClusters.foreach(println)
    return mostFatalClusters
    
  }

  // Parameter low is the lowest k and high is the highest one.
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
    val kdf = df.select("a","b")   
    val vectorAssembler = new VectorAssembler().setInputCols(Array("a","b"))
                                               .setOutputCol("features")
    // Bonus Task #4
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))   
    val pipeLine = transformationPipeline.fit(kdf)
    val transformedData = pipeLine.transform(kdf)      
    
    // Scaling data
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")  
    val scalerModel = scaler.fit(transformedData)
    val scaledData = scalerModel.transform(transformedData)
    
    // K-means models with different k's and calculating the costs
    // of clustering with different k-values. 
    val clusteringCosts: Array[(Int, Double)] = new Array[(Int, Double)](high-low+1);
    val kmeans = new KMeans().setK(low).setSeed(1L).setFeaturesCol("scaledFeatures")   
    for(i <- low to high){
      val model = kmeans.fit(scaledData)
      val predictions = model.transform(scaledData)
      val cost = model.computeCost(scaledData)
      clusteringCosts(i-low) = (i,cost)
      kmeans.setK(i+1)
    }
    
    // Results in (k,costs) form where k is amount of clusters
    // and costs is the cost of clustering with k clusters
    println("\n Task #4: (k, costs) pairs: \n")
    clusteringCosts.foreach(println)
    println("\n Task #4: Check the source code file to find the graph for elbow method for Bonus Task #5. \n")
    /* 
     * BONUS TASK 5: I implemented the visualization of elbow method with Python, 
     * since I found it too difficult to do in Scala. ( I couldn't find any library
     * or function that would have plotted my results in a way I wanted, or at least
     * as easily than using matplotlib in Python. The source code for Bonus task 5 
     * can be found from the same folder as this source code file. Image of results
     * I got can be also found there in as a 
     * 
     */
    return clusteringCosts
  }
  
  def dirtydatafounder2dim(df: DataFrame): Boolean = {
    val data = df.select("a","b")
    val dirtydataA = data.filter("a < 0")
    val dirtydataB = data.filter("b < 0")
    val n = dirtydataA.count() + dirtydataB.count()    
    if(n > 0){
      return true
    }
    return false
    
  }
  
   def dirtydatafounder3dim(df: DataFrame): Boolean = {
    val data = df.select("a","b","c")
    val dirtydataA = data.filter("a < 0")
    val dirtydataB = data.filter("b < 0")
    val dirtydataC = data.filter("c < 0")
    val n = dirtydataA.count() + dirtydataB.count() + dirtydataC.count()        
    if(n > 0){
      return true
    }
    return false
    
  }
   
   def pipelineDemo(df: DataFrame){
    val data = df.select("a","b","c")   
    val vectorAssembler = new VectorAssembler().setInputCols(Array("a","b","c"))
                                               .setOutputCol("features")
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))   
    val pipeLineModel = transformationPipeline.fit(data)
    val transformedData = pipeLineModel.transform(data)
    println("\n Bonus Task #4: Showing pipeline running in action: \n")   
    transformedData.show()
   }
  
}


