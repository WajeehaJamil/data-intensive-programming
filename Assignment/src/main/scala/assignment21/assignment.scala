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
import org.apache.spark.sql.functions.{count, sum, min, max, asc, desc, udf, to_date, avg, when, lit,stddev, mean}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansSummary}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import java.io.{PrintWriter, File}
import sys.process._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.immutable.Range
import breeze.linalg._
import breeze.numerics._
import breeze.plot._


object assignment  {
  // Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)
                       
  val spark =  SparkSession.builder()
                          .appName("assignment")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()
  import spark.implicits._
  val excludeColumnName= ("LABEL")
  
  // Bonus task 2 defining schemas instead of inferring them
  val data2DSchema = new StructType(Array(new StructField("a", DoubleType, true),
                       new StructField("b", DoubleType, true),new StructField("LABEL", StringType, true)))
  val data3DSchema = new StructType(Array(new StructField("a", DoubleType, true),
                       new StructField("b", DoubleType, true), new StructField("c", DoubleType, true),new StructField("LABEL", StringType, true)))
                          
  val dataK5D2 =  spark.read
                     .option("header","true")
                     .schema(data2DSchema)
                     .csv("data/dataK5D2.csv")
                     .drop(excludeColumnName)

  val dataK5D3 =  spark.read
                     .option("header","true")
                     .schema(data3DSchema)
                     .csv("data/dataK5D3.csv")
                     .drop(excludeColumnName)
                     
  val dataK5D3WithLabels = spark.read
                       .option("header","true")
                       .schema(data2DSchema)
                       .csv("data/dataK5D2.csv")
                       
  val dataK5D2_dirty =  spark.read
                     .option("header","true")
                     .schema(data2DSchema)
                     .csv("data/dataK5D2-dirty.csv")
                     .drop(excludeColumnName)
  
  val toDouble = udf[Double, String]( _.toDouble)

  /*
   * This function compute and return cluster means for two-dimensional data in a given number of groups (k)
   * 
   * @Param df Raw dataFrame 
   * @Param k The number of means
   * @Return An Array of the means (a,b)
   * 
   * */
  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
    
    val data = bonustask3for2Ddata(df)
    // Load data to the memory 
    data.cache()
    data.show(10)
    
    // Create a VectorAssembler for mapping input column "a" and "b" to "features" 
    // This step needed because all machine learning algorithms in Spark take as input a Vector type  
    val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("a", "b"))
    .setOutputCol("features")
   
    val transformationPipeline = new Pipeline()
                                .setStages(Array(vectorAssembler))
    
    val pipeLine = transformationPipeline.fit(data)
    val transformedTraining = pipeLine.transform(data)
    
    // Show the list of descriptive statistics 
    println("\n Summary of descriptive statistics ---")
    data.describe().show()
    
    // Scaling data is not required in two-dimensional as standard deviation for both a/b is not so high
    
    // Kmeans model fitting to the data
    val kmeans = new KMeans()
               .setK(k).setSeed(1L)
    val kmModel= kmeans.fit(transformedTraining)
    val centers = kmModel.clusterCenters.map(x=> (x(0), x(1)))
    
    return centers
  }
  
  
  /*
   * This function compute and return cluster means for three-dimensional data in a given number of groups (k)
   * 
   * @Param df Raw dataFrame 
   * @Param k The number of means
   * @Return An Array of the means (a,b,c)
   * 
   * */
  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {
    
    val data = bonustask3for3Ddata(df)
    // Load data to the memory 
    data.cache()
    data.show(10)
    
    // Show the list of descriptive statistics 
    println("\n Summary of descriptive statistics ---")
    data.describe().show()

    // The above method shows that column 'c' standard deviation is high - 273.6304.
    // So, it need to be scaled down, otherwise variance sensitive k-means can compute entirely on the basis of the 'c' 
    
    val vectorizeCol = udf( (v:Double) => Vectors.dense(Array(v)) )        
    
    // MinMaxScaler can be used to transform the 'c' column to be scale down  
    val vectorizeCData = data.withColumn("cVec", vectorizeCol(data("c")))    
    val minMaxScaler = new MinMaxScaler()
       .setInputCol("cVec")
       .setOutputCol("cScaled")
       .setMax(1)
       .setMin(-1)
    
    val scaledData = minMaxScaler
       .fit(vectorizeCData)
       .transform(vectorizeCData)
       
    
    // Create a VectorAssembler for mapping input column "a", "b" and "c" to "features"
    // This step needed because all machine learning algorithms in Spark take as input a Vector type  
    val vectorAssembler = new VectorAssembler()
        .setInputCols(Array("a", "b", "cScaled"))
        .setOutputCol("features")
   
    
    // Perform pipeline with sequence of stages to process and learn from data 
    val transformationPipeline = new Pipeline()
        .setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(scaledData)
    val transformedTraining = pipeLine.transform(scaledData)
    
    // Kmeans model fitting to the data
    val kmeans = new KMeans().setK(k).setSeed(1L)
    val kmModel= kmeans.fit(transformedTraining)
    
    val centers = kmModel.clusterCenters.map(x => (x(0), x(1), x(2)))
    centers.foreach(println)
    
    // Bonus task 6
    // Following codes are to make x(2) value in the centroids to be scaled up 
    val centersDF = spark.createDataFrame(centers)
        .toDF("c1", "c2", "c3")
   
    // Scaled back c3 cluster centroid with min and max value of 'c' column 
    val vectorDF = centersDF.withColumn("c3Vec", vectorizeCol(centersDF("c3")))    
    val minMaxScalerBack = new MinMaxScaler()
         .setInputCol("c3Vec")
         .setOutputCol("c3ScaledBack")
         .setMax(991.9577)
         .setMin(9.5387)
   
    val scaledBackCenters = minMaxScalerBack
         .fit(vectorDF)
         .transform(vectorDF)
    
    val centersDFScaleBack = scaledBackCenters.select(col("c1"), col("c2"), col("c3ScaledBack"))
      
    // Convert DataFrame to Array  
    val centersArray = centersDFScaleBack.select("c1", "c2", "c3ScaledBack").collect()
          .map(each => (each.getAs[Double]("c1"), each.getAs[Double]("c2"), each.getAs[Double]("c3ScaledBack")))
          .toArray
    return centersArray
  }
  
  
  /*
   * This function gets the means of the data grouped by the label, adds a 3rd dimension to group Fatal and Ok in
   * separate groups and get the mean of each one
   * 
   * @Param df Raw dataFrame 
   * @Param k The number of means
   * @Return An Array of the means (a,b)
   * 
   * */
  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    
    //  Add 3rd dimension based on label Ok = 0, Fatal = 1
    val parsedDF = df.withColumn("numLabel", when(col("label").contains("Ok"), 0).otherwise(1))
    
    val data = bonustask3for2Ddata(parsedDF).withColumn("numLabel", toDouble(parsedDF("numLabel")))

    // Load data to the memory 
    data.cache()
    data.show(10)
    
    
    // Create VectorAssembler for mapping input column "a", "b" and "label" to "features"
    // This step needed because ML algorithms in Spark take as input a Vector type
    val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("a", "b", "numLabel"))
    .setOutputCol("features")
   
    // Perform pipeline with sequence of stages to process and learn from data
    val transformationPipeline = new Pipeline()
        .setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(data)
    val transformedTraining = pipeLine.transform(data)
    
    // Scaling data is not required in two-dimensional as standard deviation for both a/b is not so high
    // val minMax = new MinMaxScaler().setMin(-1).setMax(1).setInputCol("features") 
    // val fittedminMax = minMax.fit(transformedTraining) 
    // val scaledData = fittedminMax.transform(transformedTraining)
    
    
    // Kmeans model fitting to the data
    val kmeans = new KMeans().setK(k).setSeed(1L)
    val kmModel= kmeans
    .setFeaturesCol("features")
    .setPredictionCol("prediction")
    .fit(transformedTraining)
    
    // Get cluster centers
    val centers = kmModel.clusterCenters
    
    // One way to get two most fatal cluster centroids
    val filtered = centers.sortBy(x => (- x(2))).take(2)
    
    //Another way 
    //val filtered = centers.filter( x => x(2) >= 0.5 )
    return filtered.map(x => ( x(0), x(1) ) )
  }

  
  /*
   * This function is used to apply the kmeans model for a range of k
   * 
   * @Param df The transformed pipeline
   * @Param n The current index of the function
   * @Param high Limit used for the base case
   * @Return An Array of pairs (k, cost) between a range
   * 
   * */
  // Bonus task 1 using recursion instead of looping
  def recursiveTask4(df: DataFrame, n: Int, high: Int): Array[(Int, Double)] = {
   
    // Kmeans model fitting to the data
    val kmeans = new KMeans().setK(n).setSeed(1L)
    val kmModel= kmeans.fit(df)
   
    // Compute the cost
    val cost = kmModel.computeCost(df)
   
    // Base Case
    if (n == high){
      return Array((n, cost))
    }else {
      return Array((n, cost)) ++: recursiveTask4(df, n + 1, high)
    }

  }
  

  // Parameter low is the lowest k and high is the highest one.
   /*
   * This function is used to apply the k-means clustering for a range of k in order to get the optimal number of the cluster means. 
   * 
   * @Param df Raw dataFrame 
   * @Param low First k to try
   * @Param high Limit of ks 
   * @Return An Array of pairs (k, cost) between a range
   * 
   * */
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
    
    val data = bonustask3for2Ddata(df)
    // No need to cache data as dataK5D2 is already cached in Task 1
    
    // Create VectorAssembler for mapping input column "a", "b" and "label" to "features"
    // This step needed because ML algorithms in Spark take as input a Vector type
    val vectorAssembler = new VectorAssembler()
        .setInputCols(Array("a", "b"))
        .setOutputCol("features")
   
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(data)
    val transformedTraining = pipeLine.transform(data)
    
    // Scaling data is not required in two-dimensional as standard deviation for both a/b is not so high
    val results = recursiveTask4(transformedTraining, low, high)
    val (clusterAmountArray, costArray) = results.unzip
    val clusterAmount = clusterAmountArray.map(k=>k.toDouble)
    
    bonustask5(costArray, clusterAmount)
    return results
  }
  
  def bonustask5(costArray: Array[Double], clusterAmountArray: Array[Double]): String  = {
    val fig = Figure()
    val p = fig.subplot(0)
    val cost = new DenseVector(costArray)
    val clusterAmount =
      new DenseVector(clusterAmountArray)
    p += plot(clusterAmount, cost)
    scala.io.StdIn.readLine() 
  }
  
  def bonustask3for2Ddata(df: DataFrame) : DataFrame = {
    // Change the a,b column variable type to double
    // Drop any rows in which any value is null in a,b column
    // Remove duplicate data
    val data = df.withColumn("a", toDouble(df("a")))
                  .withColumn("b", toDouble(df("b")))
                  .na.drop("any") 
                  .distinct
    return data
  }
  
  
  def bonustask3for3Ddata(df: DataFrame) : DataFrame = {
    // Change the a,b,c column variable type to double
    // Drop any rows in which any value is null in a,b,c column 
    // Remove duplicate data
    val data = df.withColumn("a", toDouble(df("a")))
                  .withColumn("b", toDouble(df("b")))
                  .withColumn("c", toDouble(df("c")))
                  .na.drop("any")
                  .distinct()
    return data
  }
  
  
  // Outputs
  println("\n Task 1: k-means clustering for two dimentional data: dataK5D2.csv")
  val task1_sol = task1(dataK5D2, 5)
  task1_sol.foreach(println)
    
  println("\n Task 2: k-means clustering for three dimentional data: dataK5D3.csv")
  val task2_sol = task2(dataK5D3, 5)
  task2_sol.foreach(println)
  
    
  println("\n Task 3: k-means clustering to get two most fatal cluster centroids")
  val task3_sol = task3(dataK5D3WithLabels, 5)
  task3_sol.foreach(println)
  
  println("\n Task 4: Elbow method to find the optimal number of the cluster means.")
  val task4_sol = task4(dataK5D2, 2, 10)
  task4_sol.foreach(println)
    
}


