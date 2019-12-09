package com.Busuness.statistics.analysis

import java.text.SimpleDateFormat
import java.util.Date

import com.Busuness.statistics.model.{MongoConfig, Rating}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author 王犇
  * @date 2019/12/9 9:49
  * @version 1.0
  *
  *          离线统计服务
  */
object StatisticsRecommender {


  //项目名
  val SPARK_APPLICATION_NAME = "StatisticsRecommender"


  val MONGODB_RATING_COLLECTION = "Rating"

  //历史热门商品统计
  val RATE_MORE_PRODUCT = "RateMoreProduct"

  //最近热门商品统计
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"

  //商品平均得分统计
  val AVERAGE_PRODUCTS = "AverageProducts"


  def main(args: Array[String]): Unit = {
    val config = Map(
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val conf = new SparkConf().setAppName(SPARK_APPLICATION_NAME).setMaster(args(0))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sc = sparkSession.sparkContext

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //加载数据
    loadDataToDF(sparkSession)

    //根据所有历史评分数据，计算历史评分次数最多的商品,并写入mongodb
    RateMoreProductWriteToMongo(sparkSession)

    //根据评分，按月为单位计算最近时间的月份里面评分数最多的商品集合。
    RateMoreRecentlyProduct(sparkSession)

    //    * 根据历史数据中所有用户对商品的评分，周期性的计算每个商品的平均得分。
    Average(sparkSession)

    sc.stop()
  }

  /**
    * 根据历史数据中所有用户对商品的评分，周期性的计算每个商品的平均得分。
    *
    * @param sparkSession
    * @param mongoConfig
    */
  private def Average(sparkSession: SparkSession)(implicit mongoConfig: MongoConfig) = {

    val AverageProducts = sparkSession.sql("select productId , avg(score) avg from ratings group by productId order by avg desc")


    AverageProducts.write.option("uri", mongoConfig.uri).option("collection", AVERAGE_PRODUCTS).mode(SaveMode.Overwrite).format("com.mongodb.spark.sql").save()
  }

  /**
    * 根据评分，按月为单位计算最近时间的月份里面评分数最多的商品集合。
    *
    * @param sparkSession
    */
  private def RateMoreRecentlyProduct(sparkSession: SparkSession)(implicit mongoConfig: MongoConfig) = {

    val simpleFormat = new SimpleDateFormat("yyyy-MM")

    //注册一个UDF函数，用于将timestamp装换成年月格式   1260759144000  => 201605
    sparkSession.udf.register("changeDate", (x: Long) => (simpleFormat.format(new Date(x * 1000))))

    val ratingOfYearMonth = sparkSession.sql("select  productId, score ,changeDate(timestamp) yearmonth   from ratings ")

    ratingOfYearMonth.createOrReplaceTempView("ratingOfYearMonth")

    val ratingOfYearMonthDF = sparkSession.sql("select productId,count(productId) count, yearmonth  from ratingOfYearMonth group by productId,yearmonth order by  yearmonth desc , count desc ")

    ratingOfYearMonthDF.write.mode(SaveMode.Overwrite).option("uri", mongoConfig.uri).option("collection", RATE_MORE_RECENTLY_PRODUCTS).format("com.mongodb.spark.sql").save()
  }

  /**
    * 根据所有历史评分数据，计算历史评分次数最多的商品,并写入mongodb
    *
    * @param sparkSession
    */
  private def RateMoreProductWriteToMongo(sparkSession: SparkSession)(implicit mongoConfig: MongoConfig) = {

    val RateMoreProductDF = sparkSession.sql("select productId, count(productId) as count from ratings group by productId  order by count DESC")

    RateMoreProductDF.write.mode(SaveMode.Overwrite).option("uri", mongoConfig.uri).option("collection", RATE_MORE_PRODUCT).format("com.mongodb.spark.sql").save()
  }

  /**
    * 加载数据到DataFrame
    *
    * @param sparkSession
    * @param mongoConfig
    */
  private def loadDataToDF(sparkSession: SparkSession)(implicit mongoConfig: MongoConfig) = {
    import sparkSession.implicits._

    val ratingDF = sparkSession.read.option("uri", mongoConfig.uri).option("collection", MONGODB_RATING_COLLECTION).format("com.mongodb.spark.sql").load().as[Rating].toDF()

    ratingDF.createOrReplaceTempView("ratings")
  }
}
