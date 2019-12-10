package com.Busuness.streamingRecommender.analysis

import com.Busuness.streamingRecommender.model.{MongoConfig, ProductRecs}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.Jedis

/**
  * @author 王犇
  * @date 2019/12/10 13:42
  * @version 1.0
  */
object StreamingRecommender {

  //用户推荐最多20个
  val MAX_USER_RATINGS_NUM = 20
  //商品的相似商品排行20个
  val MAX_SIM_PRODUCTS_NUM = 20
  //流数据
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  // 推荐表的名称
  val USER_RECS = "UserRecs"
  //商品的相似产品
  val PRODUCT_RECS = "ProductRecs"


  def main(args: Array[String]): Unit = {
    val config = Map(
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(args(0))
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sc = sparkSession.sparkContext

    val ssc = new StreamingContext(sc, Duration.apply(5))

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import sparkSession.implicits._

    // 广播商品相似度矩阵
    //装换成为 Map[Int, Map[Int,Double]]
    val simProductsMatrix = sparkSession.read.option("uri", mongoConfig.uri).option("collection", PRODUCT_RECS).format("com.mongodb.spark.sql").load().as[ProductRecs].rdd.map(recs => {
      (recs.productId, recs.recs.map(x => (x.productId, x.score)).toMap)
    }).collectAsMap()

    val simpProductMatrixBroadcast = sc.broadcast(simProductsMatrix)

    //创建到Kafka的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))


    // UID|MID|SCORE|TIMESTAMP
    // 产生评分流
    val ratingStream = kafkaStream.map(msg => {
      val strs = msg.value().split("\\|")
      (strs(0).toInt, strs(1).toInt, strs(2).toDouble, strs(3).toLong)
    })

    // 核心实时推荐算法
    ratingStream.foreachRDD(rdd => {
      rdd.map {
        case (userId, productId, score, timeStamp) => {
          println(">>>>>>>>>>>>>>>>")

          //获取当前最近的M次商品评分
          val userRecentlyProducts: Array[(Int, Double)] = getUserRecentlyProducts(MAX_USER_RATINGS_NUM, userId, ConnHelper.jedis)

          //获取商品P最相似的K个商品


          //计算待选商品的推荐优先级


          //将数据保存到MongoDB


        }
      }
    })
  }

  /**
    * 获取当前最近的M次商品评分
    *
    * @param MAX_USER_RATINGS_NUM
    * @param userId
    * @param jedis
    * @return
    */
  def getUserRecentlyProducts(maxNum: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
    import scala.collection.JavaConversions._
    jedis.lrange("userId:" + userId.toString, 0, maxNum).map(x => {
      val strs = x.split("\\:")(attr(0).trim.toInt, attr(1).trim.toDouble)
    }).toArray
  }
}
