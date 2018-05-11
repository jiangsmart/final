package wtist.dataClean.impl

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
/**
 * 合并SM/CC/SM数据，并对三种数据产生的异常用户过滤
 * Created by wtist on 2016/5/24.
 */
object MergeData {
  /**
   * 合并SM、CC、MM文件并且对三种数据产生的异常用户过滤
   * @param mergeData: RDD[user+","+time+","+cell+","+longitude+","+latitude+","+province]  合并后的RDD
   * @param totalAbnormalUserMap:  Broadcast[scala.collection.Map[String,Int]\] 合并后的异常用户表
   * @return RDD[user+","+time+","+cell+","+longitude+","+latitude+","+province]
   */
  def Merge(mergeData: RDD[String], totalAbnormalUserMap: Broadcast[scala.collection.Map[String,Int]]) :RDD[String] =  {
        val mergeUserWithProvinceAndCoordinate = mergeData.map(x =>
              x.split(",") match {case Array(user, time, cell, lng, lat, province) => (user,(time,cell,lng,lat,province))})
                .mapPartitions({x =>
                    val m = totalAbnormalUserMap.value
                    for{(user,(time,cell,lng,lat,province)) <- x if (!m.contains(user))}
                        yield user + "," + time + "," + cell +","+lng+","+lat+","+province
             }).distinct
          mergeUserWithProvinceAndCoordinate
       }
}
