package com.Busuness.streamingRecommender.analysis

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.{JedisPoolConfig, JedisSentinelPool}

/**
  * @author 王犇
  * @date 2019/12/10 13:44
  * @version 1.0
  */
object ConnHelper {
  lazy val sentinels = Set[String]("192.168.8.180:26379", "192.168.8.247:26379")
  lazy val jedisPoolConfig = new JedisPoolConfig
  jedisPoolConfig.setMaxTotal(200)
  jedisPoolConfig.setMaxIdle(50)

  import scala.collection.JavaConversions._

  lazy val jedisSentinelPool = new JedisSentinelPool("master", sentinels, jedisPoolConfig, 3000, "123456", 3)

  lazy val jedis = jedisSentinelPool.getResource

  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://hadoop102:27017/recommender"))
}
