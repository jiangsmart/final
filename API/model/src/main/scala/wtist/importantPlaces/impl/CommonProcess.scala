package wtist.importantPlaces.impl

import org.apache.spark.rdd.RDD
import wtist.Tools
import Tools._

/**
 * Created by wtist on 2016/5/16.
 */
@deprecated
object CommonProcess {
  def FindFrequentCell(location:RDD[String], StartTime: String, EndTime: String, format: String = "yyyyMMddHHmmss"): RDD[String] = {
      val startTime = timetostamp(StartTime,format).toLong
      val endTime = timetostamp(EndTime,format).toLong
      val record = location.map(x => x.split(",") match { case Array(user, time, cell, lat, lont, province) => (user, timetostamp(time,format).toLong, cell+","+lat+","+lont)}).
                      filter{x => x match { case (user, timestamp, cellWtihCoord) => timestamp >= startTime && timestamp < endTime}}.
                        map(x => x match {case (user, timestamp, cellWtihCoord) => (user + "#"+ ((timestamp-startTime)/1800000).toString()+"#"+cellWtihCoord, 1)}) .
                          reduceByKey(_ + _).map { x =>
                             val user_time = x._1.split("#")(0) + "#" + x._1.split("#")(1);
                             val cell = x._1.split("#")(2);
                             val count = x._2;
                            (user_time, (cell, count))}.
                     groupByKey().map { x => val sort_arr = x._2.toArray.sortWith((a, b) => a._2 > b._2); (x._1, sort_arr) }.
                       map{ x=> val len = x._2.length;
                            val cell =
                              if(len == 0){
                                    "***"
                              }
                              else{
                                    x._2(0)._1
                              }
                          (x._1,cell)
                        }.filter(!_._2.equals("***")).map(x => x._1 + "#" + x._2)
       record
  }
  def timeChange(record:RDD[String], StartTime:String, format:String="yyyyMMddHHmmss"):RDD[String]={
      val startTime = timetostamp(StartTime,format).toLong
      val SelectedUserLoc = record.map{x=>  x.split("#")match {
           case Array(user, timestamp, cellWithCoord) => (user+","+stamptotime(timestamp.toInt*1800000+startTime,format)+","+cellWithCoord)
      }}
          SelectedUserLoc
  }
}
