package wtist.importantPlaces.impl

import org.apache.spark.rdd.RDD

/**
 * Created by wtist on 2016/4/24.
 */
object HomeIdentify {
  /**
   * 获得用户的家所在的位置 定义：时间段（20:00-07:00）最频繁出现的位置
   * time 示例 20150802100001
   * @param SelectedUserLoc : RDD[String] = RDD[user + "," + timestamp + "," + cell + "," + longitude + "," + latitude + "," + province]
   * @return Home : RDD[String] = RDD[user + "," + home ]
   */
  def GetUserHomeByFreq(SelectedUserLoc: RDD[String]): RDD[String] = {
    val Home =SelectedUserLoc.map(x => x.split(",")match { case Array(user, time, cell, lng,lat, province) => (user,time,cell,lng,lat)})
                .filter { x => x match{case (user, time, cell,lng,lat) => (time.substring(8, 10).toInt >= 20) || (time.substring(8,10).toInt <= 7)} }                         //time.substring(8, 10) hour
                  .map(x => x match { case (user, time, cell,lng,lat) => (user+"#"+cell+","+lng+","+lat , 1)}).reduceByKey(_ + _).map{ x =>
                       val user = x._1.split("#")(0)
                       val cell = x._1.split("#")(1)
                       val cell_count = x._2
                      (user, (cell, cell_count))
                   }.groupByKey().map { x => val sort_arr = x._2.toArray.sortWith((a, b) => a._2 > b._2); (x._1, sort_arr) }
                       .filter(x => x._2.length >= 1).map { x => val user = x._1; val cellWithCoord = x._2(0)._1;user + "," + cellWithCoord  }
         Home
  }

  /**
   * 获得用户的家所在的位置 定义：时间段（20:00-07:00）最频繁出现的位置  注意没有省份字段
   * time 示例 20150802100001
   * @param SelectedUserLoc : RDD[String] = RDD[user + "," + timestamp + "," + cell + "," + longitude + "," + latitude]
   * @return Home : RDD[String] = RDD[user + "," + home ]
   */
  def GetUserHomeByFreqWithoutProvince(SelectedUserLoc: RDD[String]): RDD[String] = {
    val Home =SelectedUserLoc.map(x => x.split(",")match { case Array(user, time, cell, lng,lat) => (user,time,cell,lng,lat)})
                .filter { x => x match{case (user, time, cell,lng,lat) => (time.substring(8, 10).toInt >= 20) || (time.substring(8,10).toInt <= 7)} }                          //time.substring(8, 10) hour
                  .map(x => x match { case (user, time, cell,lng,lat) => (user+"#"+cell+","+lng+","+lat , 1)}).reduceByKey(_ + _).map{ x =>
      val user = x._1.split("#")(0)
      val cell = x._1.split("#")(1)
      val cell_count = x._2
      (user, (cell, cell_count))
    }.groupByKey().map { x => val sort_arr = x._2.toArray.sortWith((a, b) => a._2 > b._2); (x._1, sort_arr) }
      .filter(x => x._2.length >= 1).map { x => val user = x._1; val cellWithCoord = x._2(0)._1;user + "," + cellWithCoord  }
    Home
  }
}
