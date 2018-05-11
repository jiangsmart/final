package wtist.dataClean.impl

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import wtist.Tools._
/**
 * 去除上网卡用户，在线天数阈值和每天小时阈值
 * Created by wtist on 2016/5/24.
 */
object DeCardUsers {
  /**
   * 去除上网卡用户
   * @param data:  RDD[user+","+time+","+cell+","+longitude+","+latitude+","+province]
   * @param StartTime: 20150801000000
   * @param EndTime: 20150901000000
   * @param dayThreshold: 默认天数阈值25
   * @param hourThreshold： 默认每天小时阈值10
   * @return RDD[trueUser]
   */
  def DeCardUser(data: RDD[String], StartTime : String = "20150801000000", EndTime : String = "20150901000000", dayThreshold: Int = 25, hourThreshold: Int = 10) : RDD[String] = {
      val start_time = timetostamp(StartTime).toLong
      val end_time = timetostamp(EndTime).toLong
      val TrueUsers = data.map(x => x.split(",") match { case Array(user, time, cell, lng, lat, province) => (user, time, cell)}).
             filter{x => x match { case (user, time, cell) => timetostamp(time).toLong >= start_time && timetostamp(time).toLong < end_time}}.
               map(x => x match {case (user, time, cell) => user+","+time.substring(4, 10)}).distinct().                                             // time.substring(4, 10) => 2015(080100)0000  monthdayhour
                 map{x =>  x.split(",") match { case Array(user, day) => (user + "," + day.substring(2, 4), 1)}}.                    //day.substring(2, 4) => 08(01)00 day
                   reduceByKey(_+_).
                     filter(x => x match{case(userDay, hourCount) => hourCount >= hourThreshold}).
                       map(x => x match {case (userDay, count) => userDay}).
                         map{x =>x.split(",") match {case Array(user,day) => (user, 1)}}.
                           reduceByKey(_+_).filter(x => x match {case (user ,dayCount) => dayCount >= dayThreshold}).
                             map(x => x match {case (user, dayCount) => user})
      TrueUsers
   }

  /**
   * 获取指定用户的位置信息
   * @param data: RDD[user+","+time+","+cell+","+longitude+","+latitude+","+province]
   * @param SelectedUser: RDD[user]
   * @return RDD[user+","+time+","+cell+","+longitude+","+latitude+","+province]
   */
  def GetSelectedUserLoc(data: RDD[String], SelectedUser: RDD[String]) : RDD[String] = {
      val SelectedUserLoc = data.map(x => x.split(",")match {case Array(user, time, cell_id, lng, lat, province) => (user, (time, cell_id, lng, lat, province))})
            .join(SelectedUser.map(x => x match{case trueUser => (trueUser, 1)}))
              .map{x => x match{case (user, ((time, cell, lng, lat, province), count)) => user + "," + time + "," + cell +","+lng+","+lat+","+province}}
    SelectedUserLoc
  }


  /**
   * 获取指定用户的位置信息  map-join端 join
   * @param data: RDD[user+","+time+","+cell+","+longitude+","+latitude+","+province]
   * @param SelectedUserMap:  Broadcast[Map[String, Int]\]
   * @return RDD[user+","+time+","+cell+","+longitude+","+latitude+","+province]
   */
  def GetSelectedUserLoc(data: RDD[String], SelectedUserMap: Broadcast[scala.collection.Map[String,Int]]) : RDD[String] = {
    val SelectedUserLoc = data.map(x => x.split(",")match {case Array(user, time, cell_id, lng, lat, province) => (user,time, cell_id, lng, lat, province)})
          .mapPartitions({x =>
            val m = SelectedUserMap.value
            for{(user,time, cell, lng, lat, province) <- x if (m.contains(user)) }
              yield user + "," + time + "," + cell +","+lng+","+lat+","+province}
            )
    SelectedUserLoc
  }
}
