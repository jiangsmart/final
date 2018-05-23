package wtist.stayPoint.impl

import org.apache.spark.rdd.RDD
import wtist.Tools._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wtist on 2016/11/7.
 */
class StayRegion {
  def determineStayRegion(StablePoint: RDD[String], SpaThresh: Double, TimeThresh: Double): RDD[String] = {
    val userRecord = StablePoint.map(_.split(",")).map { x => x match {
      case Array(user,pcode, cell,lng, lat, starttime, endtime, stablesign) =>
        (user + "," + starttime.substring(0, 8), (cell, lng, lat, starttime, endtime))
    }
    }.filter(x=> !x._2._4.equals("None") && !x._2._5.equals("None")&& !x._2._2.equals("None") && !x._2._3.equals("None")).
      groupByKey().map { x =>
      val user = x._1.split(",")(0)
      val day = x._1.split(",")(1)
      val sorted_arr = x._2.toArray.sortWith((a, b) => (timetostamp(a._4).toLong < timetostamp(b._4).toLong)) //每天记录按时间排序
    var r_lng = sorted_arr(0)._2.toDouble
      var r_lat = sorted_arr(0)._3.toDouble
      val cell_arr = ArrayBuffer[String]()
      val r_arr = ArrayBuffer[String]() //lng,lat,stime,etime,cell_arr
      cell_arr += sorted_arr(0)._1
      var start_time = sorted_arr(0)._4
      var end_time = sorted_arr(0)._5
      var start_index = 0
      var i = 1
      while (i < sorted_arr.length) {
        val distance = GetDistance(r_lng, r_lat, sorted_arr(i)._2.toDouble, sorted_arr(i)._3.toDouble)
        if (distance <= SpaThresh) {
          r_lng = (r_lng * cell_arr.length + sorted_arr(i)._2.toDouble) / (cell_arr.length+1)
          r_lat = (r_lat * cell_arr.length + sorted_arr(i)._3.toDouble) / (cell_arr.length+1)
          cell_arr += sorted_arr(i)._1
          end_time = sorted_arr(i)._5
          i = i + 1
        } else {
          if ((cell_arr.distinct.size > 1)
            && ((timetostamp(end_time).toDouble - timetostamp(start_time).toDouble) > TimeThresh.toDouble)) {
            r_arr += (user+","+day+","+r_lng.toString+","+r_lat.toString+","+start_time+","+end_time+","+"True,"+cell_arr.mkString("#"))
            i = i
          } else if((cell_arr.distinct.size == 1)&&(((timetostamp(end_time).toDouble - timetostamp(start_time).toDouble) > TimeThresh.toDouble) || ((i >= 2) && ((timetostamp(start_time).toDouble - timetostamp(sorted_arr(i - 2)._4).toDouble) > TimeThresh.toDouble)) )){
            r_arr += (user+","+day+","+r_lng.toString+","+r_lat.toString+","+start_time+","+end_time+","+"True,"+cell_arr.mkString("#"))
            i = i
          } else {
            r_arr += (user+","+day+","+sorted_arr(start_index)._2+","+sorted_arr(start_index)._3+","+start_time+","+end_time+","+"False,None")
            i = start_index + 1
          }
          start_index = i
          cell_arr.clear()
          cell_arr += sorted_arr(i)._1
          r_lng = sorted_arr(i)._2.toDouble
          r_lat = sorted_arr(i)._3.toDouble
          start_time = sorted_arr(i)._4
          end_time = sorted_arr(i)._5
        }
      }
      r_arr
    }.flatMap(x => x)
    userRecord
  }
}
