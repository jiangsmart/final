package wtist.importantPlaces.impl

import org.apache.spark.rdd.RDD
import wtist.Tools
import Tools._

/**
 * Created by wtist on 2016/4/24.
 */
@deprecated
object WorkIdentify {
  def GetUserWorkByFreq(SelectedUserLoc: RDD[String]): RDD[String] = {
    val Work = SelectedUserLoc.map(x => x.split(",")match { case Array(user, time, cell, lat, lont, province) => (user, time, cell, lat, lont)}).
                  filter(x => x match {case (user, time, cell, lat, lont) => isWeekday(time.substring(0, 8)) && ((time.substring(8, 10).toInt >= 10 && time.substring(8, 10).toInt <= 12) || (time.substring(8, 10).toInt >= 14 && time.substring(8, 10).toInt <= 17))}).
                    map(x => x match { case (user, time, cell, lat, lont) => (user+"#"+cell+","+lat+","+lont, 1)}).
                      reduceByKey(_ + _).map { x =>
                          val user = x._1.split("#")(0)
                          val cell = x._1.split("#")(1)
                          val cell_count = x._2
                         (user, (cell, cell_count))}.
                     groupByKey().map { x => val sort_arr = x._2.toArray.sortWith((a, b) => a._2 > b._2); (x._1, sort_arr) }.filter(x => x._2.length >= 1).map { x => val user = x._1; val workWithCoord = x._2(0)._1;user + "," + workWithCoord}
    Work
  }

  def GetUserWorkByFreqWithoutProvince(SelectedUserLoc: RDD[String]): RDD[String] = {
    val Work = SelectedUserLoc.map(x => x.split(",")match { case Array(user, time, cell, lat, lont) => (user, time, cell, lat, lont)}).
      filter(x => x match {case (user, time, cell, lat, lont) => isWeekday(time.substring(0, 8)) && ((time.substring(8, 10).toInt >= 10 && time.substring(8, 10).toInt <= 12) || (time.substring(8, 10).toInt >= 14 && time.substring(8, 10).toInt <= 17))}).
      map(x => x match { case (user, time, cell, lat, lont) => (user+"#"+cell+","+lat+","+lont, 1)}).
      reduceByKey(_ + _).map { x =>
      val user = x._1.split("#")(0)
      val cell = x._1.split("#")(1)
      val cell_count = x._2
      (user, (cell, cell_count))}.
      groupByKey().map { x => val sort_arr = x._2.toArray.sortWith((a, b) => a._2 > b._2); (x._1, sort_arr) }.filter(x => x._2.length >= 1).map { x => val user = x._1; val workWithCoord = x._2(0)._1;user + "," + workWithCoord}
    Work
  }
}
