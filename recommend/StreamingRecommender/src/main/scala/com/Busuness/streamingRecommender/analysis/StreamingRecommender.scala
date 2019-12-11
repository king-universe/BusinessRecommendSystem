package com.Busuness.streamingRecommender.analysis

import com.Busuness.streamingRecommender.model.{MongoConfig, ProductRecs}
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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


  val MONGODB_RATING_COLLECTION = "Rating"
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

    val ssc = new StreamingContext(sc, Seconds(5))

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import sparkSession.implicits._

    // 广播商品相似度矩阵
    //装换成为 Map[Int, Map[Int,Double]]
    val simProductsMatrix = sparkSession.read.option("uri", mongoConfig.uri).option("collection", PRODUCT_RECS).format("com.mongodb.spark.sql").load().as[ProductRecs].rdd.map(recs => {
      (recs.productId, recs.recs.map(x => (x.productId, x.score)).toMap)
    }).collectAsMap()

    val simpProductMatrixBroadcast = sc.broadcast(simProductsMatrix)

    //消费者组id
    val groupId = "kafkaSparkGroup1"
    //topic信息     //map中的key表示topic名称，map中的value表示当前针对于每一个receiver接受器采用多少个线程去消费数据
    val topics = List("businessMsg")
    //4、接受topic的数据
    //配置kafka相关参数
    val kafkaParams : Map[String,Object]=Map[String,Object](
      "bootstrap.servers" -> "192.168.1.232:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafkaSparkGroup1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean));


    val kafkaStream :  InputDStream[ConsumerRecord[String,String]] =  KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String, String](topics,kafkaParams))

    // UID|MID|SCORE|TIMESTAMP
    // 产生评分流
    val ratingStream = kafkaStream.map(msg => {
      val strs = msg.value().split("\\|")
      println(s"message$strs")
      (strs(0).toInt, strs(1).toInt, strs(2).toDouble, strs(3).toLong)
    })

    // 核心实时推荐算法
    ratingStream.foreachRDD(rdd => {
      rdd.map {
        case (userId, productId, score, timeStamp) => {
          println(">>>>>>>>>>>>>>>>")

          //获取当前最近的M次商品评分
          val userRecentlyProducts: Array[(Int, Double)] = getUserRecentlyProducts(MAX_USER_RATINGS_NUM, userId, ConnHelper.jedis)

          //获取商品P最相似的K个商品,并按照评分进行排序
          val similarProducts: Array[Int] = getProductSimilarProducts(MAX_SIM_PRODUCTS_NUM, productId, userId, simpProductMatrixBroadcast.value)

          //计算待选商品的推荐优先级
          val productSortByScore: Array[(Int, Double)] = computeProductScores(simpProductMatrixBroadcast.value, userRecentlyProducts, similarProducts)

          productSortByScore.foreach(x=>println(x._1+":::::productId:"+x._2))

          //将数据保存到MongoDB
          saveRecsToMongoDB(userId, productSortByScore)

        }
      }
    })

    ssc.start()
    ssc.awaitTermination()

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
      val strs = x.split("\\:")
      (strs(0).trim.toInt, strs(1).trim.toDouble)
    }).toArray
  }

  /**
    * 获取商品P最相似的K个商品
    *
    * @param MAX_SIM_PRODUCTS_NUM 取相似的个数
    * @param productId            商品的Id
    * @param userId               用户的Id
    * @param value                相似矩阵
    * @param mongoClient          mongodb的客户端
    * @return
    */
  def getProductSimilarProducts(num: Int, productId: Int, userId: Int, value: collection.Map[Int, Map[Int, Double]])(implicit mongoConfig: MongoConfig): Array[Int] = {
    //从广播变量的商品相似度矩阵中获取当前商品所有的相似商品
    val allSimilarProduct = value.get(productId).get.toArray

    //获取用户已经观看过得商品
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).find(MongoDBObject("userId" -> userId)).toArray.map(item => item.get("productId").toString.toInt)


    //过滤掉已经评分过得商品，并排序输出
    allSimilarProduct.filter(x => {
      val flag = ratingExist.contains(x._1)
      !flag
    }).sortWith(_._2 > _._2).take(num).map(_._1)
  }


  /**
    * 计算待选商品的推荐优先级
    *
    * @param value
    * @param userRecentlyProducts
    * @param similarProducts
    * @return
    */
  def computeProductScores(value: collection.Map[Int, Map[Int, Double]], userRecentlyProducts: Array[(Int, Double)], similarProducts: Array[Int]): Array[(Int, Double)] = {
    //用于保存每一个待选商品和最近评分的每一个商品的权重得分
    var score = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    //用于保存每一个商品的增强因子数
    var increMap = scala.collection.mutable.HashMap[Int, Int]()

    //用于保存每一个商品的减弱因子数
    var decreMap = scala.collection.mutable.HashMap[Int, Int]()


    for (simProduct <- similarProducts; userProduct <- userRecentlyProducts) {
      val simScore: Double = getProductsSimScore(value, simProduct, userProduct._1)

      if (simScore > 0.6) {
        score += ((simProduct, simScore * userProduct._2))
        if (userProduct._2 > 3) {
          increMap(simProduct) = increMap.getOrElse(simProduct, 0) + 1
        } else {
          decreMap(simProduct) = decreMap.getOrElse(simProduct, 0) + 1
        }
      }
    }
    score.groupBy(_._1).map {
      case (productId, sims) => (productId, sims.map(_._2).sum / sims.length + log(increMap.getOrElse(productId, 1)) - log(decreMap.getOrElse(productId, 1)))
    }.toArray.sortWith(_._2 > _._2)
  }

  /**
    * 获取当个商品之间的相似度
    *
    * @param value        商品相似度矩阵
    * @param simProduct   商品P最相似的K个商品
    * @param recProductId 用户已经评分的商品
    * @return
    */
  def getProductsSimScore(value: collection.Map[Int, Map[Int, Double]], simProduct: Int, recProductId: Int): Double = {

    value.get(simProduct) match {
      case Some(sim) => sim.get(recProductId) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  //取10的对数
  def log(m: Int): Double = {
    math.log(m) / math.log(10)
  }

  /**
    * 将数据保存到MongoDB    userId -> 1,  recs -> 22:4.5|45:3.8
    *
    * @param streamRecs 流式的推荐结果
    * @param mongConfig MongoDB的配置
    */
  def saveRecsToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    //到StreamRecs的连接
    val streaRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    streaRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streaRecsCollection.insert(MongoDBObject("userId" -> userId, "recs" ->
      streamRecs.map(x => MongoDBObject("productId" -> x._1, "score" -> x._2))))
  }
}
