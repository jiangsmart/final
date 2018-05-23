package wtist.stayPoint.impl

import org.apache.spark.rdd.RDD
import wtist.Tools
import Tools._

import scala.collection.mutable.ArrayBuffer

@deprecated
/**
  * Created by chenqingqing on 2016/5/25.
  */
object StayPoint {
  /**
    * 将记录中的first和last的字段整合成duration,并进行去重，将相同的基站合并为一个，将duration相加
    *
    * @param Location : RDD[String] = RDD[user + "," + cell + "," +lng+","+lat+","+first+","+last]
    * @return filteredRec : RDD[String] = RDD[user + "," + firsttime + "," + cell+","+duration +"," +lng +"," +lat]
    */
  def stp1_filterSameCell(Location:RDD[String]): RDD[String] ={
    val filteredRec = Location.map(_.split(","))
      .filter{x=> x(4).equals("") ==false && x(5).equals("") ==false}
      .map{x=> x match{
      case Array(user,cell,lng,lat,first,last) =>
    val dur = (timetostamp(last,"yyyyMMddHHmmss").toLong-timetostamp(first,"yyyyMMddHHmmss").toLong)/3600000.0
        (user,(first,cell,dur.toString,lng,lat))
    }}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=>
        timetostamp(a._1,"yyyyMMddHHmmss").toDouble < timetostamp(b._1,"yyyyMMddHHmmss").toDouble)
        (x._1,FilterSameCell(arr))}
      .flatMapValues(x=>x)
      .map{x=> x._1+","+x._2}
    filteredRec

  }

  /**
    * 认定三次连续往返即为震荡，将震荡对用出现频率高的基站替代
    *
    * @param StayPoint_stp1 : RDD[String] = RDD[user + "," + timestamp + "," + cell_id+","+duration +","+lng+","+lat] ；
    * @return  RDD[String] = RDD[day+userid+time+cell+dur]
    */

  def stp2_distinctStayPoint(StayPoint_stp1:RDD[String]):RDD[String]={
    val stp1 = StayPoint_stp1.map{x=> x.split(",") match {
      case Array(user,time,cell,dur,lng,lat) => (user,time,cell,dur,lng,lat)}}
    val stp2 = stp1.map{x=>
        val day_usr = x._2.substring(0,8)+","+x._1;//day+user
      val cell = x._3
        val dur = x._4
      val lng = x._5
      val lat = x._6
        (day_usr,(timetostamp(x._2,"yyyyMMddHHmmss"),cell,dur,lng,lat))
      }
      .groupByKey()
      .map{x=> val trajArr = x._2.toArray.sortWith((a, b) => a._1.toLong < b._1.toLong)
        (x._1,FilterOscillation(trajArr))}
      .map{x=> (x._1,FilterSameCell(x._2))}//调用函数FilterSameCell
      .flatMapValues(x=>x)
      .map{x=> x._1+","+x._2}//day+","+user+","+time+","+cell+","+dur+","+lng+","+lat
    stp2
  }
  /**
    * 利用时空限制进一步地提取停留点
    *
    * @param StayPointLatLng : RDD[String] = RDD[day + "," +user + "," + timestamp + "," + cell_id+","+dur+","+longitude+","+latitude] ；
    * @return  RDD[String] = RDD[String] = RDD[day + "," +user + "," + timestamp + "," + cell_id+","+dur+","+longitude+","+latitude] ；
    */

  def stp3_timeSpatialDistrict(StayPointLatLng: RDD[String]): RDD[String] ={
    val result = StayPointLatLng.map{x=> x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) =>
        (day+","+user,(timetostamp(time,"yyyyMMddHHmmss"),cell,dur,lng,lat))
    }}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a, b) => a._1.toLong < b._1.toLong)
        (x._1,SpatialTimeDistrict(arr))}//调用函数,结果返回(day+","+user,(time,cell,dur,lng,lat))
      .map{x=> (x._1,FilterSameCell(x._2))}
      .flatMapValues(x=>x)
      .map{x=> x._1 +","+x._2}//day+","+user+","+time+","+cell+","+dur+","+lng+","+lat
    result

  }

  /**
    * 去除相邻时间的相同基站,并将duration叠加
    *
    * @param arr 已根据时间排序后的记录,Array[(String,String,String)] = (time,cell,dur,lng,lat)
    * @return Array[String] = time+","+cell+","+dur+","+lng+","+lat
    */
  def FilterSameCell(arr:Array[(String,String,String,String,String)]): Array[String] ={
    val sorted_arr=ArrayBuffer[String]();
    var record1 = arr(0)._1+","+arr(0)._2 //time+cell
    var record2 = arr(0)._4+","+arr(0)._5//lng+lat
    var recDur:Double = arr(0)._3.toDouble
    for(i<-0 until arr.length) {
      if ((i+1)<arr.length){
        val pre = arr(i)
        val cur = arr(i + 1)
        if (pre._2.equals(cur._2)) {
          recDur = recDur +  cur._3.toDouble
        } else {
          sorted_arr += record1+","+recDur+","+record2
          record1 = cur._1+","+cur._2 //
          record2 = cur._4+","+cur._5
          recDur = cur._3.toDouble


        }
      } else {
        sorted_arr += record1+","+recDur+","+record2

      }

    }
    sorted_arr.toArray
  }
  /**
    * 去除3次以上连续震荡，并用频率最高的基站替代,同时将他们的持续时间叠加
    *
    * @param cellst : Array[(String,String,String)] = [(time,cell,duration,lng,lat)]
    * @return output: Array[(String,String,String)] = [(time,cell,duration,lng,lat)]
    */

  def FilterOscillation(cellst: Array[(String,String,String,String,String)]): Array[(String,String,String,String,String)] = {
    //第一个循环：得到基站频率的Map
    val len = cellst.length
    val dist = new scala.collection.mutable.HashMap[String, Int]
    val osciDist = new scala.collection.mutable.HashMap[String,String]
    var item = ""
    for (i <- 0 until len) {
      item = cellst(i)._2
      if (dist.contains(item)) {
        dist(item) += 1
      } else {
        dist(item) = 1

      }
    }


    //第二个循环:得到震荡点对和替换点的Map
    var start = 0;
    var index = 2;
    var count = 0;
    var flag = false;
    while (index < len) {
      if ((cellst(index)._2).equals(cellst(start)._2)) {
        count += 1
        if(count >2 && flag == false){
          start += 2
        }
        if (count >= 2 && flag == false) {
          if(start >0){
            if (cellst(start - 1)._2.equals(cellst(start + 1)._2) && cellst(start + 1)._2.equals(cellst(start + 3)._2)) {
              val osciPair = cellst(start-1)._2 + cellst(start)._2
              val changePot: String = if (dist(cellst(start)._2) >= dist(cellst(start - 1)._2)) cellst(start)._2 else cellst(start - 1)._2
              osciDist(osciPair) = changePot
              flag = true
            }
            else {
              if ((start + 5) < len) {
                if (cellst(start + 1)._2.equals(cellst(start + 3)._2) && cellst(start + 3)._2.equals(cellst(start + 5)._2)) {
                  val osciPair = cellst(start)._2 + cellst(start + 1)._2
                  val changePot: String = if (dist(cellst(start)._2) >= dist(cellst(start + 1)._2)) cellst(start)._2 else cellst(start + 1)._2
                  osciDist(osciPair) = changePot
                  flag = true
                }

              }

            }
          }
          else{
            if ((start + 5) < len) {
              if (cellst(start + 1)._2.equals(cellst(start + 3)._2) && cellst(start + 3)._2.equals(cellst(start + 5)._2)) {
                val osciPair = cellst(start)._2 + cellst(start + 1)._2
                val changePot = if (dist(cellst(start)._2) >= dist(cellst(start + 1)._2)) cellst(start)._2 else cellst(start + 1)._2
                osciDist(osciPair) = changePot
                flag = true
              }

            }

          }


        }


      }
      else {
        count = 0
        start = index
        flag = false

      }
      index += 2

    }
    //第三个循环：输出
    var i = 0;
    val output = new Array[(String,String,String,String,String)](len)
    while (i < len) {
      if( i == len-1){
        output(i) = (stamptotime(cellst(i)._1.toLong,"yyyyMMddHHmmss"),cellst(i)._2,cellst(i)._3,cellst(i)._4,cellst(i)._5)
        i += 1

      }else{
        val time1 = stamptotime(cellst(i)._1.toLong,"yyyyMMddHHmmss")
        val time2 = stamptotime(cellst(i+1)._1.toLong,"yyyyMMddHHmmss")
        val cells = cellst(i)._2 + cellst(i + 1)._2
        if (osciDist.getOrElse(cells,0) != 0) {
          output(i) = (time1,osciDist(cells),cellst(i)._3,cellst(i)._4,cellst(i)._5)
          output(i+1) = (time2,osciDist(cells),cellst(i+1)._3,cellst(i+1)._4,cellst(i+1)._5)
          i += 2
        }
        else {
          output(i) = (time1,cellst(i)._2,cellst(i)._3,cellst(i)._4,cellst(i)._5)
          i += 1
        }
      }
    }
    output



  }
  /**
    * 参考《城市海量手机用户停留时空分异分析》
    * 设定距离阈值300m,时间阈值1h
    * Array[String] = time,cell,dur,lng,lat，元素已按照time排序,time是Date类型
    */
  def SpatialTimeDistrict(cellst:Array[(String,String,String,String,String)]): Array[(String,String,String,String,String)] = {
    val len = cellst.length
    var output = new ArrayBuffer[(String,String,String,String,String)]
    var record = cellst(0)
    var res = (stamptotime(cellst(0)._1.toLong,"yyyyMMddHHmmss"),cellst(0)._2,cellst(0)._3,cellst(0)._4,cellst(0)._5)
    output += res
    for(i <- 1 until len){
      val cell1 = (record._4,record._5)//得到经纬度
      val time1 = record._1.toLong//得到Long型的Date类型
      val cell2 = (cellst(i)._4,cellst(i)._5)//得到经纬度
      val time2 = cellst(i)._1.toLong//得到Long型的Date类型
      val dur = time2-time1
      val distance = GetDistance(cell1,cell2)
      if( (distance <= 0.5 ) && (dur >= 1800000)){
        res = (stamptotime(cellst(i)._1.toLong,"yyyyMMddHHmmss"),record._2,cellst(i)._3,record._4,record._5)
        output += res
      }
      else{
        res  = (stamptotime(cellst(i)._1.toLong,"yyyyMMddHHmmss"),cellst(i)._2,cellst(i)._3,cellst(i)._4,cellst(i)._5)
        output += res
        record = cellst(i)
      }

    }
    output.toArray

  }


}