package wtist

import org.apache.spark.rdd.RDD
import wtist.Tools._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

/**
  * Created by chenqingqing on 2016/3/10.
  */

//import wti.st.util.Tools._

@deprecated
object Processing {


  def SortByTimeAndUniq(LocData: RDD[String]):  RDD[String] = {
    val RddSortByTime = LocData.map(_.split(",")).filter(_.length==4).map{x => x match { case Array(time,types ,user, cell_id) => Array(time, types, user, cell_id)}}.map(x => x match { case Array(time, types, user, cell_id) => (user, (time, timetostamp(time).toLong, types, cell_id))}).groupByKey().map{
      x => val sort_arr = x._2.toArray.sortWith((a, b) => a._2 < b._2); (x._1, sort_arr)}.flatMapValues(x => x).map(x => x match { case (user, (time, timestamp, types, cell_id)) => user + "," + time + "," + types + "," +cell_id})
    RddSortByTime
  }


  /*输入：user,time,cellId*/
  def FindFrequentCell(location:RDD[String]): RDD[String] = {
    val startTime = timetostamp("20150801000000","yyyyMMddHHmmss").toLong
    val record = location.map{x=> val line = x.split(",");val usr = line(0);val time = timetostamp(line(1),"yyyyMMddHHmmss").toLong;val cellid = line(2);
      val timestamp = ((time-startTime)/1800000).toString();
      (usr+","+timestamp+","+cellid,1)
    }.reduceByKey(_ + _).map { x =>
      val user_time = x._1.split(",")(0) + "," + x._1.split(",")(1);
      val cell = x._1.split(",")(2);
      val count = x._2;
      (user_time, (cell, count))
    }.groupByKey().map { x => val sort_arr = x._2.toArray.sortWith((a, b) => a._2 > b._2); (x._1, sort_arr) }.map{
      x=> val len = x._2.length;
        var cellId = ""
        if(len == 0){
            cellId = "***"
        }
        else{
            cellId = x._2(0)._1
        }
        (x._1,cellId)
    }.filter(!_._2.equals("***")).map(x => x._1 + "," + x._2)
    record
  }
  def timeChange(record:RDD[String]):RDD[String]={
    val SelectedUserLoc = record.map{x=> val line = x.split(",");val usr = line(0);val timestamp = line(1);val cellid = line(2);
    val startTime = timetostamp("20150801000000","yyyyMMddHHmmss").toLong;
    val time = stamptotime(timestamp.toInt*1800000+startTime,"yyyyMMddHHmmss");
      usr+","+time+","+cellid}
    SelectedUserLoc
  }


  /**
   * get max distance
   * @param SelectedUserLoc : RDD[String] = RDD[user + "," + time + "," + cell_id + ","  + province_code]
   * @param CellList : RDD[String] = RDD[id+","+city+","+cellid+","+locname+","+longitude+","+latitude]
   * @param StartTime : String = "2015-03-10 00:00:00"
   * @param EndTime : String = "2015-03-10 00:00:00"
   * @return MaxDistance : RDD[String] = RDD[user+","+max_distance]
   */
  def getMaxDistanceByPeriod(SelectedUserLoc: RDD[String], CellList: RDD[String], StartTime: String, EndTime: String): RDD[String] = {
    val start_time = timetostamp(StartTime).toLong
    val end_time = timetostamp(EndTime).toLong
    val MaxDistance = SelectedUserLoc.map(_.split(",")).map(x => x match {
      case Array(user, time, cell_id, province_code) => (user, timetostamp(time).toLong, cell_id, province_code)
    }).filter { x => val timestamp = x._2; timestamp >= start_time && timestamp < end_time }.map(x => x match {
      case (user, time, cell_id, province_code) => (user, cell_id, province_code)
    }).distinct().map(x => x match {
      case (user, cell_id, province_code) => (cell_id, (user, province_code))
    }).leftOuterJoin(CellList.map(_.split(",")).map(x => x match {
      case Array(id, city, cellid, locname, longitude, latitude) => (cellid, (longitude, latitude))
    })).map { x => val user = x._2._1._1; val province_code = x._2._1._2; val cell_id = x._1; val coordinates = x._2._2.getOrElse(("None", "None")); (user + "," + province_code, (cell_id, coordinates)) }.groupByKey().map { x =>
      val arr = x._2.toArray
      val arr_without_none = ArrayBuffer[(String, (String, String))]()
      for (i <- 0 until arr.size) {
        if (!arr(i)._2._1.equals("None")) {
          arr_without_none += arr(i)
        }
      }
      var max_distance = 0.0
      if (arr_without_none.size >= 2) {
        for (i <- 0 until arr_without_none.size) {
          for (j <- (i + 1) until arr_without_none.size) {
            val distance = GetDistance(arr_without_none(i)._2._1.toDouble, arr_without_none(i)._2._2.toDouble, arr_without_none(j)._2._1.toDouble, arr_without_none(j)._2._2.toDouble)
            if (max_distance < distance) {
              max_distance = distance
            }
          }
        }
      } else {
        max_distance = 100000.0
      }
      x._1 + "," + max_distance
    }
    MaxDistance
  }

//  /**
//   *
//   * @param Record : RDD[String] = RDD[user + "," + time + "," + cell_id + "," + latitude + "," + longtitude +  "," + province_code]
//   * @return
//   */
//  def Oscillation(Record : RDD[String], Separator : String = Env.COMMA) : RDD[String] = {
//    val result = Record.map(_.split(Separator)).
//                 map(x => x match {case Array(user, time, cell_id, longitude, latitude, province) => (user + "," + time.substring(6,8), (time, cell_id, longitude, latitude, province))}).  // time = "20150801001010", val day = time.substring(6,8)
//                 groupByKey().map{x =>
//                 val user_day = x._1
//                 val cell_time = x._2.toArray
//
//                }
//        result
//  }

  /**
   *  获取静态点
   *  规则：
   *  至少出现三个及以上的重复点，且sp.last - sp.first >= timeThreshold
   * @param Record : Array[(time, cell_id, longitude, latitude)]
   * @param timeThreshold : Long 时间阈值 单位：分钟
   * @return Array[(cell_id, longitude, latitude, first, last)]
   */
  def GetStablePoint(Record: Array[(String, String, String, String)], timeThreshold : Long) : Array[(String, String, String, String, String)] = {
    val total = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String)]()
    val stable = scala.collection.mutable.ArrayBuffer[(String, String, String, String)]()
    for(i <- 0 until Record.length) {
        if(stable.length == 0) {
          stable += Record(i)
        } else if(stable(0)._2.equals(Record(i)._2)){
            stable += Record(i)
        } else {
            if(IsStablePoint(stable.toArray, timeThreshold)) {
              val first = stable(0)._1
              val last = stable(stable.length - 1)._1
              val record = stable(0) match{
                case (time, cell_id, longitude, latitude) =>
                            (cell_id, longitude, latitude, first, last)
              }
              total += record
            } else {
              total ++= stable.map( x => x match { case (time, cell_id, longitude, latitude) =>
                                (cell_id, longitude, latitude, time, "")})
              stable.clear()
              stable += Record(i)
            }
        }
    }
     if(!stable.isEmpty && IsStablePoint(stable.toArray, timeThreshold)) {
        val first = stable(0)._1
        val last = stable(stable.length - 1)._1
        val record = stable(0) match{
            case (time, cell_id, longitude, latitude) =>
                (cell_id, longitude, latitude, first, last)
        }
        total += record
       } else {
        total ++= stable.map( x => x match { case (time, cell_id, longitude, latitude) => (cell_id, longitude, latitude, time, "")})
         stable.clear()
       }
    total.toArray
   }

  /**
   * 判断是否是停留点
   *  规则：
   *  至少出现三个及以上的重复点，且sp.last - sp.first >= timeThreshold
   * @param Record : Array[(time, cell_id, longitude, latitude)]
   * @param timeThreshold : Long 时间阈值 单位：分钟
   * @return 如果是静态点，则返回true，否则false
   */
  def IsStablePoint(Record : Array[(String, String, String, String)], timeThreshold : Long): Boolean = {
      val Len = Record.length
      val diff = TimeDiff(Record(0)._1, Record(Len - 1)._1 )
      if(Len < 3) {
        return false
      } else if(diff >= (60 * timeThreshold)) {
        return true
      }
      return false
  }

  /**
   * 基站去震荡规则1
   * TimeDiff(sp(i + 1).first, sp(i).last) <= timeThreshold 则去除sp(i)-sp(i+1)中间的点
   * @param Record : Array[(cell_id, longitude, latitude, first, last)]
   * @param timeThreshold : 时间阈值 单位:分钟
   * @return Array[(cell_id, longitude, latitude, first, last)]
   */
  def OscillationRule1(Record: Array[(String, String, String, String, String)], timeThreshold : Long): Array[(String, String, String, String, String)] = {
    var preRecord = ("", "", "", "", "")
    val tmp = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String)]()
    val total = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String)]()
    for(i <- 0 until Record.length) {
      if (!preRecord._1.equals("") && !Record(i)._5.equals("")) {
        if (TimeDiff(preRecord._5, Record(i)._4) <= (60 * timeThreshold)) {
          total += Record(i)
        } else {
          total ++= tmp
          total += Record(i)
          tmp.clear()
        }
      }
      if (Record(i)._5.equals("")){
        if (preRecord._1.equals("")) {
            total += Record(i)
        } else {
            tmp += Record(i)
        }
      }
      if (preRecord._1.equals("") && !Record(i)._5.equals("")) {
          preRecord = Record(i)
          total += Record(i)
      }
    }
    if(tmp.length != 0) {
        total ++= tmp
        tmp.clear()
    }
    return total.toArray
  }

  /**
   * 基站去震荡规则2
   * TimeDiff(sp(i).last, record(j)) <= (60 * timeThreshold)) && (GetDistance((Record(i), preRecord) ) >= disThreshold
   * @param Record
   * @param timeThreshold
   * @param disThreshold
   * @return
   */
  def OscillationRule2(Record: Array[(String, String, String, String, String)], timeThreshold : Long, disThreshold : Long): Array[(String, String, String, String, String)] = {
      var preRecord = ("", "", "", "", "")
      val total = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String)]()
      for(i <- 0 until Record.length) {
        if (!preRecord._1.equals("") && !Record(i)._5.equals("")) {
            preRecord = Record(i)
            total += Record(i)
        }
        if(!preRecord._1.equals("")) {
          if(Record(i)._5.equals("")) {
            if(!((TimeDiff(preRecord._5, Record(i)._4) <= (60 * timeThreshold)) && (GetDistance((Record(i)._2, Record(i)._3), (preRecord._2, preRecord._3) ) >= disThreshold))) {
                total += Record(i)
            }
          }
        }
        if(preRecord._1.equals(""))  {
          if(Record(i)._5.equals("")) {
            total += Record(i)
          } else {
            preRecord = Record(i)
            total += Record(i)
          }
        }
      }
    total.toArray
  }

  /**
   * 基站去震荡规则3
   * GetSpeed(record(i), record(i+1)) * GetSpeed(record(i+1), record(i+2)) >= speedThreshold*speedThreshold &&
   * GetDistance(record(i), record(i+1)) >= distThreshold && GetDistance(record(i+1), record(i+2)) >= distThreshold &&  GetDistance(record(i), record(i+2)) <= distThreshold / 2
   * @param Record
   * @param speedThreshold
   * @param distThreshold
   * @return
   */
  def OscillationRule3(Record: Array[(String, String, String, String, String)], speedThreshold : Double, distThreshold : Double): Array[(String, String, String, String, String)] = {
      val total = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String)]()
      val windows = scala.collection.mutable.Queue[Int]()
      if(Record.length >= 3) {
        var start = 0
        var index = 0
        while(windows.length < 3 && index < Record.length) {
            if(Record(start)._5.equals("")) {
                windows.enqueue(start)
                start += 1
            } else {
                while (windows.length > 0) {
                  total += Record(windows.dequeue())
                }
                start = 0
            }
            index += 1
        }
        for(i <- index until Record.length) {
          if(windows.length == 3) {
            if(IsRule3(windows, Record, speedThreshold, distThreshold)) {
               total += Record(windows.dequeue())
               windows.dequeue()
             } else {
               total += Record(windows.dequeue())
          }
        }
         if(Record(start)._5.equals("")) {
              windows.enqueue(start)
          } else {
            while (windows.length > 0) {
              total += Record(windows.dequeue())
            }
          }
      }
        if(windows.length == 3) {
            if(IsRule3(windows, Record, speedThreshold, distThreshold)) {
              total += Record(windows.dequeue())
              windows.dequeue()
              total += Record(windows.dequeue())
          } else {
              while (windows.length > 0) {
                total += Record(windows.dequeue())
              }
          }
        } else if(windows.length > 0) {
            while (windows.length > 0) {
              total += Record(windows.dequeue())
           }
        }
      } else {
        total ++= Record
      }
     total.toArray
  }
  def IsRule3(windows: Queue[Int], Record: Array[(String, String, String, String, String)], speedThreshold: Double, distThreshold: Double): Boolean = {
      val dist12 = GetDistance((Record(windows(0))._2,Record(windows(0))._3), (Record(windows(1))._2,Record(windows(1))._3))
      val dist13 = GetDistance((Record(windows(0))._2,Record(windows(0))._3), (Record(windows(2))._2,Record(windows(2))._3))
      val dist23 = GetDistance((Record(windows(1))._2,Record(windows(1))._3), (Record(windows(2))._2,Record(windows(2))._3))
      val speed12 = GetSpeed(dist12, Record(windows(0))._4, Record(windows(1))._4)
      val speed13 = GetSpeed(dist13, Record(windows(2))._4, Record(windows(2))._4)
      val flag = if((speed12*speed13 > (speedThreshold*speedThreshold)) && dist12 >= distThreshold && dist23 >= distThreshold && dist13 <= (distThreshold / 2)) {
          true
        } else {
          false
      }
    flag
  }
//  def OscillationRule4(Record: Array[(String, String, String, String, String)], timeWindows : Long, countThreshold : Int, uniqCount: Int): Array[(String, String, String, String, String)] = {
//      val preWindows = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String)]()
//      val windows = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String)]()
//      val nextWindows = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String)]()
//      val total = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String)]()
//
//      for(i <- 0 until Record.length) {
//         if(windows.length == 0) {
//            windows += Record(i)
//         } else {
//           if(TimeDiff(windows(0)._4,Record(i)._4) <= timeWindows) {
//              windows += Record(i)
//           } else {
//             if(preWindows.length != 0) {
//
//             }
//           }
//         }
//      }
//  }



    /**
   * 计算用户离某个地点的最大距离
   * @param SelectedUserLoc : RDD[String] = RDD[user + "," + time + "," + cell_id + ","  + province_code]
   * @param Home: RDD[String] = RDD[user + "," + home + "," + province_code]
   * @param CellList : RDD[String] = RDD[id+","+city+","+cellid+","+locname+","+longitude+","+latitude]
   * @param StartTime : String = "2015-03-10 00:00:00"
   * @param EndTime : String = "2015-03-10 00:00:00"
   * @return MaxDistance : RDD[String] = RDD[user+","+radius]
   */
  def GetMaxDistance(SelectedUserLoc: RDD[String], Home: RDD[String], CellList: RDD[String], StartTime: String, EndTime: String): RDD[String] = {
    val start_time = timetostamp(StartTime).toLong
    val end_time = timetostamp(EndTime).toLong
    val MaxDistance = SelectedUserLoc.map(_.split(",")).map(x => x match {
      case Array(user, time, cell_id) => (cell_id, (user, timetostamp(time).toLong))
    }).filter { x => val timestamp = x._2._2; timestamp >= start_time && timestamp < end_time }.leftOuterJoin(CellList.map(_.split(",")).map(x => x match {
      case Array(id, city, cellid, locname, longitude, latitude,radius, region) => (cellid, (longitude, latitude))
    })).map { x => val user = x._2._1._1; val cell_id = x._1; val coordinates = x._2._2.getOrElse(("None", "None")); (user, (cell_id, coordinates)) }.leftOuterJoin(Home.map(_.split(",")).map(x => x match {
      case Array(user, cell_id) => (cell_id, (user))
    }).leftOuterJoin(CellList.map(_.split(",")).map(x => x match {
      case Array(id, city, cellid, locname, longitude, latitude,radius, region) => (cellid, (longitude, latitude))
    })).map { x => val user = x._2._1; val cell_id = x._1; val coordinates = x._2._2.getOrElse(("None", "None")); (user, (cell_id, coordinates)) }).groupByKey().map { x =>
      val arr_list = x._2.toArray;
      var maxDistance = 0.0
      val value =
        if ((!arr_list(0)._2.getOrElse(("None", ("None", "None")))._1.equals("None")) && (!arr_list(0)._2.getOrElse(("None", ("None", "None")))._2._1.equals("None"))) {
          val home_coor = arr_list(0)._2.getOrElse(("None", ("None", "None")))._2
          val arr_ = ArrayBuffer[Double]()
          for (elem <- arr_list.toArray) {
            if (!elem._1._2._1.equals("None")) {
              val distance = GetDistance(elem._1._2._1.toDouble, elem._1._2._2.toDouble, home_coor._1.toDouble, home_coor._2.toDouble)
              if(distance > maxDistance) {
                  maxDistance = distance
              }
            }
          }
          x._1 + "," + maxDistance
        } else {
          x._1 + "," + "100000.0"
        }
      value
    }
    MaxDistance
  }

  def GetMaxDistanceByEveryDay(SelectedUserLoc: RDD[String], Location: RDD[String], CellList: RDD[String], StartTime: String, EndTime: String): RDD[String] = {
    val start_time = timetostamp(StartTime).toLong
    val end_time = timetostamp(EndTime).toLong
    //val startTime = timetostamp("20150801000000").toLong
    val MaxDistance = SelectedUserLoc.map(_.split(",")).map(x => x match {
      case Array(user, time, cell_id) => (cell_id, (user, timetostamp(time).toLong, ((timetostamp(time).toLong - start_time) / 86400).toString))
    }).filter { x => val timestamp = x._2._2; timestamp >= start_time && timestamp < end_time }.leftOuterJoin(CellList.map(_.split(",")).map(x => x match {
      case Array(id, city, cellid, locname, longitude, latitude,radius, region) => (cellid, (longitude, latitude))
    })).map { x => val user = x._2._1._1; val day  = x._2._1._3; val cell_id = x._1; val coordinates = x._2._2.getOrElse(("None", "None")); (user, (cell_id, coordinates, day)) }.leftOuterJoin(Location.map(_.split(",")).map(x => x match {
      case Array(user, cell_id) => (cell_id, (user))
    }).leftOuterJoin(CellList.map(_.split(",")).map(x => x match {
      case Array(id, city, cellid, locname, longitude, latitude,radius, region) => (cellid, (longitude, latitude))
    })).map { x => val user = x._2._1; val cell_id = x._1; val coordinates = x._2._2.getOrElse(("None", "None")); (user, (cell_id, coordinates)) }).map{x => val user = x._1; val day = x._2._1._3; val cellId = x._2._1._1; val coor = x._2._1._2; val home = x._2._2.getOrElse(("None", ("None", "None"))); (user+"#"+day,((cellId, coor), home))}.groupByKey().map { x =>
      val arr_list = x._2.toArray;
      var maxDistance = 0.0
      val value =
        if ((!arr_list(0)._2._1.equals("None")) && (!arr_list(0)._2._2._1.equals("None"))) {
          val home_coor = arr_list(0)._2._2
          val arr_ = ArrayBuffer[Double]()
          for (elem <- arr_list.toArray) {
            if (!elem._1._2._1.equals("None")) {
              val distance = GetDistance(elem._1._2._1.toDouble, elem._1._2._2.toDouble, home_coor._1.toDouble, home_coor._2.toDouble)
              if(distance > maxDistance) {
                maxDistance = distance
              }
            }
          }
          (x._1 ,maxDistance)
        } else {
          (x._1, "100000.0")
        }
      value
    }.map{x =>  val li = x._1.split("#"); val user = li(0); val time = stamptotime(li(1).toInt*86400+start_time*1000,"yyyyMMddHHmmss"); user+","+time+","+x._2}
    MaxDistance
  }

  /**
   * 输出用户一天的轨迹(尚未提取停留点)
   *
   * @param StayPoint_stp1 : RDD[String] = RDD[user + "," + timestamp + "," + cell_id] ；
   * @param day :String;  指定的日期
   * @return  RDD[String] = RDD[day+userid+cell_list+time_list]
   */
  def Trajectory(StayPoint_stp1:RDD[String],day:String):RDD[String]={
    val stp1 = StayPoint_stp1.map{x=> var line = x.split(",");(line(0),line(1),line(2))}.filter{x=>x._3 != ""}
    val everyDayTrajectory = stp1.filter{x=>x._2.substring(0,8) == day}.map{x=>
      var usr = x._1;var cell = x._3;
      (usr,timetostamp(x._2,"yyyyMMddHHmmss")+","+cell)}.groupByKey().repartition(500).map{x=> var trajArr = x._2.toArray.sortWith((a, b) => a.split(",")(0).toLong < b.split(",")(0).toLong);(x._1,trajArr)}
    val result = everyDayTrajectory.map{
      x=> var trajArr = x._2;var celllst ="";var timelst = "";
        for(i <- 0 until trajArr.length){
          if (i == trajArr.length-1){
            celllst += trajArr(i).split(",")(1)
            timelst += stamptotime(trajArr(i).split(",")(0).toLong,"yyyyMMddHHmmss")
          }
          else{
            celllst += trajArr(i).split(",")(1)+","
            timelst += stamptotime(trajArr(i).split(",")(0).toLong,"yyyyMMddHHmmss")+","
          }
        }
        "20150801&"+x._1+"&"+celllst+"&"+timelst
    }
    result

  }

}