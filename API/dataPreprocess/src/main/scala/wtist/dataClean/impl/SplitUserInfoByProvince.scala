package wtist.dataClean.impl

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * 根据省份字段对用户进行分群，具体分成海南本地用户、177用户、外省其他用户
 * Created by wtist on 2016/5/24.
 */
object SplitUserInfoByProvince {
  /**
   * 按province(省份)字段对数据集进行划分， 具体分成海南本地用户、177用户、外省其他用户
   * @param mergeUserWithProvinceAndCoordinate: RDD[user+","+time+","+cell+","+longitude+","+latitude+","+province]
   * @param province: 省份 309:海南 327:177用户
   * @return (RDD[String], RDD[String], RDD[String]) = (teleHainan, tele177, teleOthers)
   */
  def SplitUserByProvince(mergeUserWithProvinceAndCoordinate: RDD[String],  province: String = "309"): (RDD[String], RDD[String], RDD[String]) = {
      val teleHainan = mergeUserWithProvinceAndCoordinate.filter(x =>
                  x.split(",") match { case Array(user, time, cell, lng, lat, province) => province.equals(province)})
      val tele177 = mergeUserWithProvinceAndCoordinate.filter(x =>
                  x.split(",") match { case Array(user, time, cell, lng, lat, province) => province.equals("327")})
      val teleOthers = mergeUserWithProvinceAndCoordinate.filter(x =>
                  x.split(",") match { case Array(user, time, cell, lng, lat, province) => (!province.equals(province))&&(!province.equals("327"))})
      (teleHainan, tele177, teleOthers)
  }

  def SplitUserByProvinceMap(mergeUserWithProvinceAndCoordinate: RDD[String],  brocastProvinceMap: Broadcast[scala.collection.Map[String, String]], state: String = "海南" ): (RDD[String], RDD[String], RDD[String]) = {
    val provinceMap = brocastProvinceMap.value
    val teleHainan = mergeUserWithProvinceAndCoordinate.map(x =>  x.split(",") match { case Array(user, time, cell, lng, lat, province) => (user, time, cell, lng, lat, province)}).filter(x =>
      x match { case (user, time, cell, lng, lat, province) => province.equals(state) }).map(x => x match { case (user, time, cell, lng, lat, province) => (user+","+time+","+cell+","+lng+","+lat+","+provinceMap.getOrElse(province, "309"))})
    val tele177 = mergeUserWithProvinceAndCoordinate.map(x =>  x.split(",") match { case Array(user, time, cell, lng, lat, province) => (user, time, cell, lng, lat, province)}).filter(x =>
      x match { case (user, time, cell, lng, lat, province) => province.equals("177")}).map(x => x match { case (user, time, cell, lng, lat, province) => (user+","+time+","+cell+","+lng+","+lat+","+provinceMap.getOrElse(province, "327"))})
    val teleOthers = mergeUserWithProvinceAndCoordinate.map(x =>  x.split(",") match { case Array(user, time, cell, lng, lat, province) => (user, time, cell, lng, lat, province)}).filter(x =>
      x match { case (user, time, cell, lng, lat, province) => (!province.equals(state))&&(!province.equals("177")) &&  provinceMap.contains(province)}).map(x => x match { case (user, time, cell, lng, lat, province) => (user+","+time+","+cell+","+lng+","+lat+","+provinceMap.getOrElse(province, "None"))})
    (teleHainan, tele177, teleOthers)
  }
}
