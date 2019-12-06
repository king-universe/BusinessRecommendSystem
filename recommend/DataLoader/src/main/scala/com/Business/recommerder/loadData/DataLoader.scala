package com.Business.recommerder.loadData

import com.Business.recommerder.model.{MongoConfig, Product, Rating}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.Logger

/**
  * @author 王犇
  * @date 2019/12/6 10:39
  * @version 1.0
  */
object DataLoader {
  val logger = LogManager.getLogger("DataLoader")

  val SPARK_APPLICATION_NAME = "businessDataLoad"
  val PRODUCT_CSV_PATH = "D:\\workspace\\BusinessRecommendSystem\\recommend\\DataLoader\\src\\main\\resources\\products.csv"
  val RATINGS_CSV_PATH = "D:\\workspace\\BusinessRecommendSystem\\recommend\\DataLoader\\src\\main\\resources\\ratings.csv"
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      return
    }

    val conf = new SparkConf().setAppName(SPARK_APPLICATION_NAME).setMaster(args(0))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    val productRDD = sc.textFile(PRODUCT_CSV_PATH)
    val productDF = productRDD.map(item => {
      val attr = item.split("\\^")
      Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF

    val ratingsRDD = sc.textFile(RATINGS_CSV_PATH)
    val ratingsDF = ratingsRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toLong)
    }).toDF

    ratingsDF.createOrReplaceTempView("ratings")
    sparkSession.sql("select * from ratings").head(10).map(row => println(row))


    implicit val mongoConfig = MongoConfig("hadoop102:27017/recommender", "recommender")
    storeDataInMongoDB(productDF, ratingsDF)

    sc.stop()
  }

  def storeDataInMongoDB(productDF: DataFrame, ratingsDF: DataFrame)(implicit mongoConfig: MongoConfig) = {

    //新建一个到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 定义通过MongoDB客户端拿到的表操作对象
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    //如果mongodb中已经存在  应当删除
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //将当前数据写入到MongoDB
    productDF.write.option("uri", mongoConfig.uri).option("collection", MONGODB_PRODUCT_COLLECTION).mode(SaveMode.Overwrite).format("com.mongodb.spark.sql").save()
    productDF.write.option("uri", mongoConfig.uri).option("collection", MONGODB_RATING_COLLECTION).mode(SaveMode.Overwrite).format("com.mongodb.spark.sql").save()


    productCollection.createIndex(MongoDBObject("productId" -> 1))

    ratingCollection.createIndex(MongoDBObject("userId" -> 1))

    ratingCollection.createIndex(MongoDBObject("productId" -> 1))

    mongoClient.close()
  }
}

