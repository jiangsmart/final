package wtist.stayPoint.impl

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import wtist.Tools._

/**
 * Created by wtist on 2016/11/7.
 */
object TripExtraction {
  def tripExtraction(Stop: RDD[String], TimeThreshold: Double): RDD[String] = {
    val trip = Stop.map(x => x.split(",") match {
      case Array(day, user, time, cell, duration, lng, lat) => (user + "#" + day, (lng, lat, time, duration))
    }).filter(x => !x._2._1.equals("None") && !x._2._2.equals("None")).groupByKey().map {
      x =>
        val sortedArr = x._2.toArray.sortWith((a, b) => (timetostamp(a._3).toLong < timetostamp(b._3).toLong))
        var i = 1
        val tmp_arr = ArrayBuffer[String]()
        val result = ArrayBuffer[String]()
        var preStop = sortedArr(0)
//        var preIndex = -1
//        var preCoordinate = "None"
//        var curIndex = -1
//        var curCoordinate = "None"
        if (sortedArr.length == 2) {
//          preIndex = squareIndex(preStop._1.toDouble, preStop._2.toDouble)
//          preCoordinate = indexToCenterCoordinate(preIndex)
//          curIndex = squareIndex(sortedArr(1)._1.toDouble, sortedArr(1)._2.toDouble)
//          curCoordinate = indexToCenterCoordinate(curIndex)
//          result += (x._1 + "#" + preStop._1 + "," + preStop._2 + "," + preStop._3 + "," + preStop._4 + "#"+preIndex+ "," +  preCoordinate  + "#"+ sortedArr(1)._1 + "," + sortedArr(1)._2 + "," + sortedArr(1)._3 + "," + sortedArr(1)._4 +"#" + curIndex + "," + curCoordinate+ "#" + tmp_arr.mkString("$"))
           result += (x._1 + "#" + preStop._1 + "," + preStop._2 + "," + preStop._3 + "," + preStop._4 + "#" + sortedArr(1)._1 + "," + sortedArr(1)._2 + "," + sortedArr(1)._3 + "," + sortedArr(1)._4 + "#" + tmp_arr.mkString("$"))

        } else {
          while (i < sortedArr.length) {
            if (sortedArr(i)._4.toDouble >= TimeThreshold) {
//              preIndex = squareIndex(preStop._1.toDouble, preStop._2.toDouble)
//              preCoordinate = indexToCenterCoordinate(preIndex)
//              curIndex = squareIndex(sortedArr(1)._1.toDouble, sortedArr(1)._2.toDouble)
//              curCoordinate = indexToCenterCoordinate(curIndex)
//              result += (x._1 + "#" + preStop._1 + "," + preStop._2 + "," + preStop._3 + "," + preStop._4 + "#" +preIndex + "," + preCoordinate + "#"+ sortedArr(i)._1 + "," + sortedArr(i)._2 + "," + sortedArr(i)._3 + "," + sortedArr(i)._4 + "#" + curIndex + ","+curCoordinate + "#" + tmp_arr.mkString("$"))
              result += (x._1 + "#" + preStop._1 + "," + preStop._2 + "," + preStop._3 + "," + preStop._4 + "#" + sortedArr(i)._1 + "," + sortedArr(i)._2 + "," + sortedArr(i)._3 + "," + sortedArr(i)._4 + "#" + tmp_arr.mkString("$"))
              preStop = sortedArr(i)
              tmp_arr.clear()
            }
            else if (i != (sortedArr.length - 1)) {
              tmp_arr += (sortedArr(i)._1 + "," + sortedArr(i)._2 + "," + sortedArr(i)._3 + "," + sortedArr(i)._4)
            }
            i = i + 1
          }
          if (tmp_arr.length > 0 || sortedArr(i-1)._4.toDouble < TimeThreshold) {
//            preIndex = squareIndex(preStop._1.toDouble, preStop._2.toDouble)
//            preCoordinate = indexToCenterCoordinate(preIndex)
//            curIndex = squareIndex(sortedArr(i - 1)._1.toDouble, sortedArr(i - 1)._2.toDouble)
//            curCoordinate = indexToCenterCoordinate(curIndex)
//            result += (x._1 + "#" + preStop._1 + "," + preStop._2 + "," + preStop._3 + "," + preStop._4 + "#"+preIndex+","+preCoordinate +"#"+ sortedArr(i - 1)._1 + "," + sortedArr(i - 1)._2 + "," + sortedArr(i - 1)._3 + "," + sortedArr(i - 1)._4+"#"+curIndex + "," + curCoordinate + "#" + tmp_arr.mkString("$"))
              result += (x._1 + "#" + preStop._1 + "," + preStop._2 + "," + preStop._3 + "," + preStop._4 + "#" + sortedArr(i - 1)._1 + "," + sortedArr(i - 1)._2 + "," + sortedArr(i - 1)._3 + "," + sortedArr(i - 1)._4 + "#" + tmp_arr.mkString("$"))

          }
          tmp_arr.clear()
        }
        result
    }.flatMap(x => x)
    trip
  }
  def ODGenerate(Trip: RDD[String]) : RDD[String] = {
    val od = Trip.map(x => x.split("#", -1) match {
      case Array(user, day, stop1, stop2, passingList) => (stop1,stop2)
    }).map{ x =>
      val startStop = x._1.split(",")
      val endStop = x._2.split(",")
      val time = GetHalfTime(startStop(2))
      val startIndex = squareIndex(startStop(0).toDouble,startStop(1).toDouble)
      val startCoordinate = indexToCenterCoordinate(startIndex)
      val endIndex = squareIndex(endStop(0).toDouble, endStop(1).toDouble)
      val endCoordinate = indexToCenterCoordinate(endIndex)
      (time+"#"+startIndex+","+startCoordinate, endIndex+","+endCoordinate)
    }.groupByKey().map { x =>
      val Freq = x._2.foldLeft(Map[String, Int]())((m, c) => m + (c -> (m.getOrElse(c, 0) + 1)))
      val li = x._1.split("#")
      val time = li(0)
      val startPoint = li(1)
     // val weight = x._2.filter(t => !t.equals(startPoint)).size
      val result = ArrayBuffer[(String, String, String, Int)]()
      for((k,v) <- Freq) {
        if(!k.equals(startPoint)) {
          result += ((time, startPoint, k, v))
        }
      }
      result
    }.flatMap(x => x)
    val node = od.map{x =>
      val result = ArrayBuffer[(String, Int)]()
      val time = x._1
      val startPoint = x._2
      val endPoint = x._3
      val weight = x._4
      result+=((time+"#"+startPoint, weight))
      result+=((time+"#"+endPoint, weight))
    }.flatMap(x => x).groupByKey().map{
      x =>
         val weight = x._2.reduce(_+_)
        (x._1, weight)
    }
    val result = od.map(x => x match {case (time, startPoint, endPoint, weight) => (time+"#"+startPoint, (endPoint, weight))}).join(node).map{
      x =>
        val li = x._1.split("#")
        val time = li(0)
        val startPoint = li(1)
        val endPoint = x._2._1._1
        val weight = x._2._1._2
        val nodeWeight = x._2._2
        (time+"#"+endPoint, (startPoint+","+nodeWeight,weight))
    }.join(node).map{
      x =>
        val li = x._1.split("#")
        val time = li(0)
        val endPoint = li(1)
        val startPoint = x._2._1._1
        val weight = x._2._1._2
        val nodeWeight = x._2._2
        time+"#"+startPoint+"#"+endPoint+","+nodeWeight+"#"+weight
    }
    result
  }
  def GetHalfTime(time: String) :String = {
     val hour = time.substring(10,12).toInt
     val newTime = if(hour >= 30) {
        time.substring(0,10)+"3000"
     } else {
       time.substring(0,10)+"0000"
     }
    newTime
  }

}
