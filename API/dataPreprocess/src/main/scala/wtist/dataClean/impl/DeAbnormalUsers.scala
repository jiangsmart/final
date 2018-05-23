package wtist.dataClean.impl

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * 去除异常用户，防止数据倾斜问题
 * Created by wtist on 2016/5/24.
 */
object DeAbnormalUsers {
  /**
   *  根据阈值将记录分成真实用户和异常用户
   * @param data: RDD[user+","+time+","+cell+","+longitude+","+latitude+","+province]
   * @param threshold: 对于MM数据，默认阈值为10000; 对于CC数据，默认阈值为2000; 对于SM数据， 默认阈值为1000
   * @return (RDD[String], RDD[String]) = (trueUser, )
   */
  def DeAbnormalUser(data : RDD[String], threshold: Int) : (RDD[String], RDD[String]) = {
      val cleanedUser = data.map(x => x.split(",") match {case Array(user, time, cell, lng, lat, province) => (user+","+province, 1)})
            .reduceByKey(_+_)

      val abnormalUser =  cleanedUser.filter(x => x match {case (user, count) => count > threshold})
            .map(x => x match {case (user, count) => user})

      val normalUser = cleanedUser.filter(x => x match {case (user, count) => count <= threshold})
            .map(x => x match {case (user, count) => user})
    (normalUser, abnormalUser)
  }

  /**
   * 在记录中过滤掉异常用户
   * @param data: RDD[user+","+time+","+cell+","+longitude+","+latitude+","+province]
   * @param abnormalUserMap:  Broadcast[Map[String, Int]\]
   * @return RDD[user+","+time+","+cell+","+longitude+","+latitude+","+province]
   */
  def GetTrueUserWithProvinceAndCoordinate(data: RDD[String], abnormalUserMap: Broadcast[scala.collection.Map[String,Int]]): RDD[String] = {
    val TrueUserWithProvinceAndCoordinate = data.map(x => x.split(",") match {case Array(user, time, cell, lng, lat, province) => (user,(time,cell,lng,lat,province))})
          .mapPartitions({x =>
              val m = abnormalUserMap.value
              for{(user,(time,cell,lng,lat,province)) <- x if (!m.contains(user))}
                  yield user + "," + time + "," + cell +","+lng+","+lat+","+province })
    TrueUserWithProvinceAndCoordinate
  }
}
