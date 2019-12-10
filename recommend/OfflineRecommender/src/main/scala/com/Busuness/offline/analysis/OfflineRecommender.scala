package com.Busuness.offline.analysis

import com.Busuness.offline.model._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
  * @author 王犇
  * @date 2019/12/9 13:25
  * @version 1.0
  */
object OfflineRecommender {


  val SPARK_APPLICATION_NAME = "OfflineRecommender"

  // 定义常量
  val MONGODB_RATING_COLLECTION = "Rating"

  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val conf = new SparkConf().setAppName(SPARK_APPLICATION_NAME).setMaster(args(0))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

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

    //用户的数据集 RDD[Int]
    val userRDD = ratingRDD.map(_._1).distinct()
    //商品的数据集 RDD[Int]
    val productRDD = ratingRDD.map(_._2).distinct()

    val tranData = ratingRDD.map(x => Rating(x._1, x._2, x._3.toFloat))

    val (rank, iterations, lambda) = (500, 10, 0.001)

    // 调用ALS算法训练隐语义模型
    val model = ALS.train(tranData, rank, iterations, lambda)


    //计算商品相似度矩阵
    //获取商品的特征矩阵，数据格式 RDD[(scala.Int, scala.Array[scala.Double])]
    val productFeatures = model.productFeatures.map {
      case (productId, feature) => (productId, new DoubleMatrix(feature))
    }

    val productRecs = productFeatures.cartesian(productFeatures).filter {
      case (a, b) => (a._1 != b._1)
    }.map {
      case (a, b) => {
        val simScore: Double = this.consinSim(a._2, b._2) // 求余弦相似度
        (a._1, (b._1, simScore))
      }
    }.filter(_._2._2 > 0.6)
      .groupByKey()
      .map { case (productId, items) => ProductRecs(productId, items.toList.map(x => Recommendation(x._1, x._2))) }.toDF()


    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //计算用户推荐矩阵
    val userProducts = userRDD.cartesian(productRDD)

    // model已训练好，把id传进去就可以得到预测评分列表RDD[Rating] (userId,productId,rating)
    val predictRDD = model.predict(userProducts)

    val userRecs = predictRDD.filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (userId, recs) => UserRecs(userId, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()
    userRecs.show(100)
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //TODO：计算商品相似度矩阵

    // 关闭spark
    sparkSession.stop()


  }

  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    //norm2是指l2范数   就是1~n的平方和开根号  dot是向量的点乘
    product1.dot(product2) / (product1.norm2() * product2.norm2())
  }
}
