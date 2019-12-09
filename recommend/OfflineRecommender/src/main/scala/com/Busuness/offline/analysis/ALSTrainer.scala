package com.Busuness.offline.analysis

import breeze.numerics.sqrt
import com.Busuness.offline.analysis.OfflineRecommender.{MONGODB_RATING_COLLECTION, SPARK_APPLICATION_NAME}
import com.Busuness.offline.model.{MongoConfig, ProductRating}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author 王犇
  * @date 2019/12/9 15:41
  * @version 1.0
  */
object ALSTrainer {

  val SPARK_APPLICATION_NAME = "ALSTrainer"


  def main(args: Array[String]): Unit = {
    val config = Map(
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val conf = new SparkConf().setAppName(SPARK_APPLICATION_NAME).setMaster(args(0))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sc = sparkSession.sparkContext

    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import sparkSession.implicits._

    val ratingRDD = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating => (rating.userId, rating.productId, rating.score))
      .cache()

    val splits = ratingRDD.map(x => Rating(x._1, x._2, x._3.toFloat)).randomSplit(Array(0.7, 0.3))

    val tranData = splits(0)

    val testData = splits(1)

    //输出最优参数
    adjustALSParams(tranData, testData)

    //关闭Spark
    sparkSession.close()

  }


  def adjustALSParams(tranData: RDD[Rating], testData: RDD[Rating]) = {
    val result = for (rank <- Array(100, 200, 500); lambda <- Array(1, 0.1, 0.01, 0.001)) yield {
      val model = ALS.train(tranData, rank, 10, lambda)
      val rmse = getRMSE(model, testData)
      (rank, lambda, rmse)
    }
    println(result.sortBy(_._3).head)
  }

  def getRMSE(model: MatrixFactorizationModel, testData: RDD[Rating]): Double = {
    val testRDD = testData.map(ratting => (ratting.user, ratting.product))

    val predictRating = model.predict(testRDD)

    val real = testData.map(item => ((item.user, item.product), item.rating))

    val predict = predictRating.map(pre => ((pre.user, pre.product), pre.rating))
    sqrt(
      real.join(predict).map {
        case ((userId, productId), (real, predict)) => {
          val err = predict - real
          err * err
        }
      }.mean()
    )
  }
}
