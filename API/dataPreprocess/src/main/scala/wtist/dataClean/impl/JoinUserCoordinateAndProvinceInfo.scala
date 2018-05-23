package wtist.dataClean.impl

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
/**
 *  join 用户省份和经纬度表
 * Created by wtist on 2016/5/23.
 */
object JoinUserCoordinateAndProvinceInfo {
  /**
   * 加入用户经纬度信息
   * @param data: RDD[user+","+time+","+cell+","+cdr_type]
   * @param teleUserInfo: RDD[(user, province)]
   * @param broadCastCellMap:  Broadcast[Map[String, (String, String)]\]
   * @return user+","+time+","+cell+","+longitude+","+latitude+","+province
   */
  def GetUserCoordinateAndProvince(data: RDD[String], teleUserInfo: RDD[(String, String)], broadCastCellMap: Broadcast[scala.collection.Map[String,(String, String)]]): RDD[String] = {
    val dataWithProvince = GetUserProvinceInfo(data, teleUserInfo)
    val dataWithCoordinateandProvince = dataWithProvince.map(x =>
      x match {case (user, (time, cell, province)) => (cell, (user, time, province))})
      .mapPartitions({x =>
        val m = broadCastCellMap.value                         //使用了map-side join加快速度
        for{(cell,(user, time, province)) <- x}
          yield user + "," + time + "," + cell +","+m.get(cell).getOrElse(("None", "None"))._1 + "," + m.get(cell).getOrElse(("None", "None"))._2+","+province})
    dataWithCoordinateandProvince
  }


  /**
   * 加入用户经纬度信息  map-join优化
   * @param data: RDD[user+","+time+","+cell+","+cdr_type]
   * @param teleUserInfo: Broadcast[Map[user, province]\]
   * @param broadCastCellMap:  Broadcast[Map[cell, (longitude, latitude)]\]
   * @return user+","+time+","+cell+","+longitude+","+latitude+","+province
   */
  def GetUserCoordinateAndProvince(data: RDD[String], teleUserInfo: Broadcast[scala.collection.Map[String, String]], broadCastCellMap: Broadcast[scala.collection.Map[String,(String, String)]]): RDD[String] = {
    val dataWithProvince = GetUserProvinceInfo(data, teleUserInfo)
    val dataWithCoordinateandProvince = dataWithProvince
      .map(x => x match {case (user, (time, cell, province)) => (cell, (user, time, province))})
      .mapPartitions({x =>
        val m = broadCastCellMap.value                         //使用了map-side join加快速度
        for{(cell,(user, time, province)) <- x}
          yield user + "," + time + "," + cell +","+m.get(cell).getOrElse(("None", "None"))._1 + "," + m.get(cell).getOrElse(("None", "None"))._2+","+province})
    dataWithCoordinateandProvince
  }

  /**
   * 给用户记录标记省份信息
   * @param data: RDD[user+","+time+","+cell+","+cdr_type]
   * @param teleUserInfo: RDD[(user, province)]
   * @return RDD[(user, (time, cell, province))]
   */
  def GetUserProvinceInfo(data: RDD[String], teleUserInfo: RDD[(String, String)]) : RDD[(String, (String, String, String))] = {
    val dataWithProvince = data.map(x => x.split(",") match{case Array(user, time, cell, cdr_type) => (user, (time, cell))})
      .join(teleUserInfo)
      .map( x => x match{case (user, ((time, cell), province)) => (user, (time, cell, province))})
    dataWithProvince
  }
  /**
   * 给用户记录标记省份信息             使用map-side join
   * @param data: RDD[user+","+time+","+cell+","+cdr_type]
   * @param teleUserInfo: :  Broadcast[Map[String, String]\]
   * @return RDD[(user, (time, cell, province))]
   */
  def GetUserProvinceInfo(data: RDD[String], teleUserInfo: Broadcast[scala.collection.Map[String, String]]) : RDD[(String, (String, String, String))] = {
    val dataWithProvince = data.map(x => x.split(",") match{case Array(user, time, cell, cdr_type) => (user, (time, cell))})
      .mapPartitions({ x =>
        val m = teleUserInfo.value
        for {(user, (time, cell)) <- x}
          yield (user, (time, cell, m.get(user).getOrElse("None")))
      })
    dataWithProvince
  }
}
