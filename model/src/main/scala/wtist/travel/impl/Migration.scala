package wtist.travel.impl

import org.apache.spark.rdd.RDD
import wtist.Tools
import Tools._


/**
  * Created by chenqingqing on 2016/6/1.
  */
object Migration {

  /**
    * 指定日期的景区迁徙流
    * @param certainDay:String 指定的日期 yyyyMMddHHmmss
    * @param OtherProvStopPoint:RDD[String] = day+","+user +","+ time +","+ cell +","+ dur +","+ lng +","+ lat +","+ prov
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @param Scenic:RDD[String] = scenicid+","+name+","+scenicBDlng+","+scenicBDlat+","+class+","+cellid+","+cellBDlng+","+cellBDlat

    * @return
    */
  def migrationBetweenScenicSpot(certainDay:String,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[String] ={
    val scenic = Scenic.map{x=> val line = x.split(",")
      val cell = line(5).trim
      val scenicId = line(0).trim
      (cell,scenicId)}.filter{x=> x._2.matches("[0-9]+")}
    val customer = CustomerInfo.map{x=> val line = x.split(",")
      val user = line(0)
      (user,1)}
    val sp = OtherProvStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,time,cell,dur)
    }}
      .filter{x=> val day = x._1;val hour = x._3.substring(8,10);val dur = x._5.toDouble
        dur >= 1 &&
          hour.toInt >= 7 &&
          hour.toInt <= 20 &&
          day.equals(certainDay)}
      .map{case(day,user,time,cell,dur) =>(user,(cell,time))}
      .join(customer)//过滤出游客
      .map{case (user, ((cell, time), 1)) => (cell,(user,time))}
      .distinct()
      .join(scenic)
      .map{case (cell, ((user,time), scenic)) => (user,time,scenic)}.repartition(10)
    val result = sp.cartesian(sp)
      .filter{x=> val user1 = x._1._1 ;val user2 = x._2._1
        val time1 = timetostamp(x._1._2,"yyyyMMddHHmmss").toLong
        val time2 = timetostamp(x._2._2,"yyyyMMddHHmmss").toLong
        val scenicSpot1 = x._1._3;val scenicSpot2 = x._2._3
        user1.equals(user2) && time1<time2 && scenicSpot1.equals(scenicSpot2) == false
      }
      .map{case ((user1,time1,scenicSpot1),(user2,time2,scenicSpot2)) =>
        (user1,(scenicSpot1,scenicSpot2))}
      .distinct()
      .map{case(user1,(scenicSpot1,scenicSpot2)) => ((scenicSpot1,scenicSpot2),1)}
      .reduceByKey(_+_)
      .map{case ((scenicSpot1,scenicSpot2),count) =>  certainDay+","+scenicSpot1+","+scenicSpot2+","+count}
    result


  }
  /**
    * 指定月份，合并过后的每天的景区迁徙流
    *
    * @param month:String 指定的月份 yyyyMM
    * @param OtherProvStopPoint:RDD[String] = day+","+user +","+ time +","+ cell +","+ dur +","+ lng +","+ lat +","+ prov
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @param Scenic:RDD[String] = scenicid+","+name+","+scenicBDlng+","+scenicBDlat+","+class+","+cellid+","+cellBDlng+","+cellBDlat

    * @return mergeMigrationDataByDay:RDD[String] = date+","+scenicSpot1+","+scenicSpot2+","+count
    */
  def mergeMigrationDataByDay(month:String,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[String] ={
    var firstDay = migrationBetweenScenicSpot(month+"01",OtherProvStopPoint,CustomerInfo,Scenic).repartition(1)
    val dates = getDatesArray(month+"02",month+"03")
    for (day <- dates){
      val currentDay = migrationBetweenScenicSpot(day,OtherProvStopPoint,CustomerInfo,Scenic).repartition(1)
      val result = firstDay.union(currentDay)
      firstDay = result

    }
    firstDay


  }
  /**
    * 指定月份，按月求和的景区迁徙流
    * @param mergedMigrationDataWithDate:RDD[String] = date+","+scenicSpot1+","+scenicSpot2+","+count

    * @return mergeMigrationDataByMonth:RDD[String] = scenicSpot1+","+scenicSpot2+","+count
    */
  def mergeMigrationDataByMonth(mergedMigrationDataWithDate:RDD[String]):RDD[String] ={

    val result = mergedMigrationDataWithDate.map{x=> x.split(",") match{
      case Array(day,scenicSpot1,scenicSpot2,count) => ((scenicSpot1,scenicSpot2),count.toInt)
    }}
      .reduceByKey(_+_)
      .map{case ((scenicSpot1,scenicSpot2),count) => scenicSpot1+","+scenicSpot2+","+count}
    result

  }

}
