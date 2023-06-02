package ds.insights

import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.io.{BufferedOutputStream, PrintWriter}


object AmazonProductsAnalytics {
  val spark = SparkSession
    .builder()
    .appName("AmazonProductsClustering")
    .master("local[4]") /*todo comment out for aws EMR cluster runs*/
    .getOrCreate()

  import spark.implicits._

  def jsonDataLoader(filePath: String): DataFrame = {
    spark
      .read
      .format("json")
      .option("inferSchema", "true")
      .load(filePath)
      .toDF
  }

  def findTopKCategory(productsMetadataDF: DataFrame, k: Int): Array[String] = {
    // Explode first time to access list inside list of categories (the list is nested twice)
    val productCategoriesDF = productsMetadataDF
      .select($"asin", explode($"categories"))
    // Explode again to break up the list of categories, group by category to get get counts across all products
    val categoryCountsDF = productCategoriesDF
      .select($"asin", explode($"col"))
      .groupBy("col")
      .count()
      .orderBy(desc("count"))
      .limit(k)
      .select("col")
      .collect.map(row => row.getString(0))
    categoryCountsDF
  }

  def findAvgProductRating(reviewsDF: DataFrame): DataFrame = {
    reviewsDF
      .groupBy("asin")
      .mean("overall")
      .withColumnRenamed("avg(overall)", "averageRating")
  }

  def findNumProductRatings(reviewsDF: DataFrame): DataFrame = {
    reviewsDF
      .groupBy("asin")
      .count()
      .withColumnRenamed("count", "numRatings")
  }

  def findProductFeatures(productsRatingsDF: DataFrame,
                          productsMetadataDF: DataFrame): DataFrame = {
    val feat_cols = List("asin", "categories", "price", // todo: edit features here
      "averageRating", "numRatings"
    )
    productsRatingsDF
      .join(productsMetadataDF, Seq("asin"), "inner")
      .select(feat_cols.map(col): _*)
  }

  def prepareDataForClustering(productData: DataFrame, topCategories: Array[String], spreadVal: Double): DataFrame = {
    val createFeatureVector = udf {
      (categories: Seq[Seq[String]], averageRating: Int, numRatings: Int, price: Float /*todo: Add any other features from the data*/) =>
        val scaledRating = spreadVal * averageRating
        val scaledNumRatings = spreadVal * numRatings
        val scaledPrice = spreadVal * price
        val categoriesSeq = categories(0).toList
        val topCats = categoriesSeq.intersect(topCategories)
        val topCatIndices = topCats.map(cat => topCategories.indexOf(cat))
        val catVector = new Array[Double](topCategories.length) // make vector of all zeros for top categories
        topCatIndices.map(idx => catVector(idx) = 1)
        val featuresVector = catVector ++ Array(scaledRating.toDouble, scaledNumRatings.toDouble, scaledPrice.toDouble) //todo: update features modified here
        featuresVector
    }
    val data = productData.filter($"price".isNotNull).filter("price is not null").toDF
    val dfWithFeatures = data.withColumn("features",
      createFeatureVector(productData("categories"), //todo: edit features here as well
        productData("averageRating"),
        productData("numRatings"),
        productData("price")
      )
    )
    //dfWithFeatures.show(20)
    dfWithFeatures
  }

  def clusterUsingKmeans(data: DataFrame, k: Int, seed: Long): KMeansModel = {
    // Trains a k-means model and then return it
    val kmeans = new KMeans
    kmeans.setK(k)
      .setSeed(seed)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
    val kmModel = kmeans.fit(data)
    kmModel
  }

  def printKmeansCentersDetailed(model: KMeansModel, featureDF: DataFrame): Unit = {
    val predictions = model.transform(featureDF)

    // writes features and cluster assignments out to csv for visualization
    val catsToString = udf {
      (categories: Seq[Seq[String]]) =>
        categories(0).mkString(" | ")
    }

    val predictionsClean = predictions.withColumn("categoriesTmp",
      catsToString(predictions("categories"))) // https://stackoverflow.com/a/43137532
      .drop("categories")
      .withColumnRenamed("categoriesTmp", "categories")
    val out_cols = predictionsClean.columns.filter(x => x != "features")
    val predictionsOut = predictionsClean.select(out_cols.map(col): _*)
    predictionsOut.write.option("header", "true").csv("out/features_clusters.csv")

    // write cluster product counts out to csv
    val pred_counts = predictions.groupBy("prediction")
      .count()
    pred_counts.coalesce(1).write.option("header", "true").csv("out/cluster_counts.csv")
  }

  def printKmeansCenters(model: KMeansModel, categories: Array[String], outFilePath: String, spreadVal: Double): Unit = {
    /** Update me! * */
    // Shows the result.
    println("Printing Cluster Centers")
    val out = new PrintWriter(outFilePath)
    model
      .clusterCenters
      .foreach { cluster =>
        val v = cluster.toArray.splitAt(categories.length)
        v._1.zip(categories).filter(_._1 != 0).map(p => p._2).foreach(c => out.print(c + " * "))
        v._2.map(a => (a / spreadVal).round).foreach(c => out.print(c + " * "))
        out.println
      }
    out.close()
  }

  /**
   * Writes cluster centers to HDFS
   * Use when running on EMR cluster
   */
  def printKmeansCentersCluster(model: KMeansModel, categories: Array[String], outFilePath: String, spreadVal: Double): Unit = {
    import org.apache.hadoop.fs.{FileSystem,Path}
    //get the hdfs information from spark context
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    //create outFile in HDFS given the file name
    val outFile = hdfs.create(new Path(outFilePath))
    //create a BufferedOutputStream to write to the file
    val out = new BufferedOutputStream(outFile)
    model
      .clusterCenters
      .foreach { cluster =>
        val v = cluster.toArray.splitAt(categories.length)
        v._1.zip(categories).filter(_._1 != 0).map(p => p._2).foreach(c => out.write((c + " * ").getBytes("UTF-8")))
        v._2.map(a => (a / spreadVal).round).foreach(c => out.write((c + " * ").getBytes("UTF-8")))
        out.write('\n')
      }
    out.close()
  }

  def evaluateModel(model: KMeansModel, dataset: DataFrame): Unit = {
    import org.apache.hadoop.fs.{FileSystem,Path}
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    //create outFile in HDFS given the file name
    val outFile = hdfs.create(new Path("out/scores.txt"))
    //create a BufferedOutputStream to write to the file
    val out = new BufferedOutputStream(outFile)
    val predictions = model.transform(dataset)
    val k = model.getK

    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    val colnames = dataset.columns.mkString(", ")
    out.write(("Silhouette with squared euclidean distance | "
      + silhouette + " | " + colnames + " | "
      + k)
      .getBytes("UTF-8"))
    out.close()
  }

  def getRatingsFeatures(amazonReviewsDF: DataFrame): DataFrame = {
    val ratingDF = findAvgProductRating(amazonReviewsDF)
    val numRatingsDF = findNumProductRatings(amazonReviewsDF)
    ratingDF.join(numRatingsDF, Seq("asin"), "inner") //todo: replace this when removing review features

  }

  /**
   * clustering workflow: preprocess feature vectors and run k-means
   */
  def clusterAmazonData(amazonReviewsDF: DataFrame, amazonMetadataDF: DataFrame, topKCategories: Array[String], spreadValue: Int, clustersNumber: Int): KMeansModel = {
    val ratingDF = getRatingsFeatures(amazonReviewsDF)
    val featuresDF = findProductFeatures(
      ratingDF,
      amazonMetadataDF)
    val featuresColDF = prepareDataForClustering(featuresDF,
      topKCategories, spreadValue)
    val kmeansModel = clusterUsingKmeans(featuresColDF, clustersNumber, seed = 1L)
    evaluateModel(kmeansModel: KMeansModel, featuresColDF)
    kmeansModel
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val persist = if (args.length > 0) if (args(0).toInt == 1) true else false else false
    println("Persist = " + persist)

    if (!config.hasPath("Analytics.ProductInsights.Amazon.reviewsFilePath") || !config.hasPath("Analytics.ProductInsights.Amazon.metadataFilePath")) {
      println("Error, configuration file does not contain the file paths for reviews and metadata files!")
      spark.close()
    }

    val amazonReviewsFilePath = config.getString("Analytics.ProductInsights.Amazon.reviewsFilePath")
    val amazonMetadataFilePath = config.getString("Analytics.ProductInsights.Amazon.metadataFilePath")

    val amazonReviewsDF = jsonDataLoader(amazonReviewsFilePath)
    val amazonMetadataDF = jsonDataLoader(amazonMetadataFilePath)
    if (persist == true) {
      val reviewsDF = jsonDataLoader(amazonReviewsFilePath)
      val metadataDF = jsonDataLoader(amazonMetadataFilePath)
      val amazonReviewsDF = reviewsDF.persist()
      val amazonMetadataDF = metadataDF.persist()
      println("Persisting the dataframes.")
    }
    else {
      val amazonReviewsDF = jsonDataLoader(amazonReviewsFilePath)
      val amazonMetadataDF = jsonDataLoader(amazonMetadataFilePath)
      println("Loading dataframes without persist.")
    }

    //generate an array of the top k categories to be considered as features for k-means clustering
    //val numCategoriesAsFeatures = config.getInt("Analytics.ProductInsights.Amazon.numCategoriesAsFeatures")
    val numCategoriesAsFeatures = if (args.length > 1) args(1).toInt else if (config.hasPath("Analytics.ProductInsights.Amazon.numCategoriesAsFeatures")) config.getInt("Analytics.ProductInsights.Amazon.numCategoriesAsFeatures") else 20
    println("number of Categories as features:" + numCategoriesAsFeatures)

    val clustersNumber = if (args.length > 2) args(2).toInt
    else if (config.hasPath("Analytics.ProductInsights.Amazon.clustersNumber")) config.getInt("Analytics.ProductInsights.Amazon.clustersNumber")
    else numCategoriesAsFeatures * 3
    println("number of clusters for kmeans clustering:" + clustersNumber)

    val topKCategories = findTopKCategory(amazonMetadataDF, numCategoriesAsFeatures)
    println(topKCategories.mkString("\n"))

    val spreadValue = config.getInt("Analytics.ProductInsights.Amazon.spreadValue")
    val outFilePath = config.getString("Analytics.ProductInsights.Amazon.clusterCentersOutFilePath")

    val kmeansModel = clusterAmazonData(amazonReviewsDF, amazonMetadataDF, topKCategories, spreadValue, clustersNumber)

    printKmeansCenters(kmeansModel, topKCategories, outFilePath, spreadValue)
    /*todo comment out for cluster runs*/

    val featuresDF = findProductFeatures(getRatingsFeatures(amazonReviewsDF), amazonMetadataDF) /*todo comment out for cluster runs*/
    if (persist == true) {
      amazonMetadataDF.unpersist()
      amazonReviewsDF.unpersist()
      println("Unpersisting the dataframes.")
    }
    val featuresColDF = prepareDataForClustering(featuresDF, topKCategories, spreadValue) /*todo comment out for cluster runs*/
    printKmeansCentersDetailed(kmeansModel, featuresColDF) /*todo comment out for cluster runs*/
    //print or evaluate the model
    //evaluateModel(kmeansModel, prepareDataForClustering(findProductFeatures(findAvgProductRating(amazonReviewsDF), amazonMetadataDF), topKCategories, spreadValue))
    spark.close()
  }

}