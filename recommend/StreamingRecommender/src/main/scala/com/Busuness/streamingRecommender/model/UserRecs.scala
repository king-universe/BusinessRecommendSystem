package com.Busuness.streamingRecommender.model

/**
  * 用户的推荐
  * @author 王犇
  * @date 2019/12/9 14:00
  * @version 1.0
  */
case class UserRecs(userId: Int, recs: Seq[Recommendation])
