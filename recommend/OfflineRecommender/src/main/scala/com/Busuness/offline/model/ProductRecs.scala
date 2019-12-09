package com.Busuness.offline.model

/**
  * 商品相似度（商品推荐）
  * @author 王犇
  * @date 2019/12/9 15:09
  * @version 1.0
  */
case class ProductRecs(productId: Int, recs: Seq[Recommendation])
