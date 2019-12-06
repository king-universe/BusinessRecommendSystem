package com.Business.recommerder.model

/**
  * @author 王犇
  * @date 2019/12/6 10:42
  * @version 1.0
  */
case class Rating(userId: Int, productId: Int, score: Double, timestamp: Long)
