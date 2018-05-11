package wtist.travel.impl

import org.apache.spark.rdd.RDD
import wtist.Tools
import Tools._


@deprecated
/**
  * Created by chenqingqing on 2016/5/26.
  */
object Travel {
  /**
    * 提取用户去除工作地点的停留点记录
    *
    * @param  Work:RDD[String] = RDD[usr + "," + cellid]
    * @return  RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat]//所有
    */
  def stayPointWithoutWork(Work:RDD[String],StayPoint:RDD[String]):RDD[String] ={
    val work = Work.map{x=> val line = x.split(",");(line(0)+","+line(1),1)}.distinct()
    val stayPoint1 = StayPoint.map{x=> val line = x.split(",");(line(1)+","+line(3),x)}
      .leftOuterJoin(work)
      .map{x=> val flag = x._2._2.getOrElse(0);(x._2._1,flag)}
      .filter{x=> x._2 == 0}
      .map{ x=> x._1}
    stayPoint1


  }

  /**
    * 计算用户每日离家最远距离，不包括工作地点
    * 算法描述：首先去除工作地点的记录，然后过滤出停留时间大于1h的记录。对这些记录计算离家距离，并得到某用户某天的最大距离。
    *
    * @param StayPointWithOutWork : RDD[String] = RDD[day + "," +user + "," + timestamp + "," + cell_id+","+dur+ "," +longitude+ "," +latitude] ；
    * @param Home:RDD[String] = RDD[usr + "," + cellid + "," +longitude+ "," +latitude+","+flag]
    * @param Work:RDD[String] = RDD[usr + "," + cellid + "," +longitude+ "," +latitude+","+flag]
    * @return  RDD[String] = RDD[day + "," +user+","+ maxDistance] ；
    */

  def MaxHomeDistanceEveryDay(StayPointWithOutWork:RDD[String],Home:RDD[String],Work:RDD[String]): RDD[String] ={
    val sp = StayPointWithOutWork.map{x=> x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (user,(day,(lng,lat)),dur)
    }}
      .filter{x=> val dur=x._3;dur.toDouble >= 1}
      .map{case (user,(day,(lng,lat)),dur) => (user,(day,(lng,lat)))}
    val home = Home.map{x=>val line = x.split(",")
      val user = line(0)
      val lng = line(2)
      val lat = line(3)
      (user,(lng,lat))}
      .distinct()
    val result = home.join(sp)
      .map{x=> val homelnglat = x._2._1
        val loclnglat = x._2._2._2
        val distance = GetDistance(homelnglat,loclnglat)
        val user = x._1
        val day = x._2._2._1
        (day+","+user,distance)}
      .filter{x=> x._2 < 100000.0} //经纬度不存在时会返回100000.0
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b) => a > b )
        val dayAndUser = x._1
        val maxDis = arr(0)
        dayAndUser+","+maxDis
    }

    result

  }
  /**
    * 计算用户工作日(过滤周末)离家平均最远距离(工作日生活半径)
    *
    * @param  HomeDistanceEveryDay: RDD[String] = RDD[ day +","+ user + "," + distance] ；
    * @return  RDD[String] = RDD[user + "," + weekdayAveMaxDistance] ；
    */

  def MaxHomeAveDistanceWorkDay(HomeDistanceEveryDay:RDD[String],User:RDD[String]): RDD[String] ={
    val homeDistance = HomeDistanceEveryDay.map{x=>val line = x.split(",");(line(1),(line(0),line(2)))}
      .filter{x=> val day = x._2._1;isWeekday(day)}
      .groupByKey()
      .repartition(500)
      .map{x=>
        val arr = x._2.toArray
        var arrSum:Double = 0
      for(i <- 0 until arr.length){
        arrSum += arr(i)._2.toDouble
      }
      val ave = arrSum/arr.length
      x._1+","+ave}//平均每天最远离家距离，有些用户的家没有经纬度，因此，计算的时候lifeCircle是0
    val user = User.map{x=>(x,1)}
    val lifecircle = homeDistance.map{x=> val line = x.split(",")
      val user = line(0)
      val cell = line(1)
      (user,cell)}
    val output = user.leftOuterJoin(lifecircle)
      .map{x=> val flag = x._2._2.getOrElse(0)
        x._1+","+flag}
    output.saveAsTextFile("/user/chenqingqing/location/LifeCircleRadius")
    output

  }


  /**
    * 提取用户的出行记录,停留时间大于1小时，不认定为旅游
    *
    * @param  StayPointWithoutWork:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+ "," +longitude+ "," +latitude] ；
    * @param  MaxDis:RDD[String] = user+","+distance
    * @param  Home:RDD[String] = RDD[usr + "," + cellid + "," +longitude+ "," +latitude]
    * @return  RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat+","+maxdis+","+dis]//所有
    */
  def travelOutsideLifecircle(StayPointWithoutWork:RDD[String],Home:RDD[String],MaxDis:RDD[String]):RDD[String] ={
    val sp = StayPointWithoutWork.map{x=> val line = x.split(",");(line(0),x)}
      .filter{x=> val day = x._1;isWeekday(day)}
      .map{x=> val line = x._2.split(",")
        val user = line(1)
        val lng = line(5)
        val lat = line(6)
        (user,(x._2,(lng,lat)))}
    val home = Home.map{x=>val line = x.split(",")
        val user = line(0)
        val lng = line(2)
        val lat = line(3)
        (user,(lng,lat))}
        .distinct()
    val result = home.join(sp)
      .map{x=> val homelnglat = x._2._1
        val loclnglat = x._2._2._2
        val distance = GetDistance(homelnglat,loclnglat)
        val user = x._1
        val line = x._2._2._1
        (user,(line,distance))}
      .filter{x=> x._2._2 < 100000.0}//(user,(info,distance))
    val maxDistance = MaxDis.map{x=> val line = x.split(",");(line(0),line(1))}.distinct()
    val output  = maxDistance.join(result)
      .filter{x=> val dis = x._2._2._2
        val max = x._2._1.toDouble
        dis > max}
      .map{x=>val dur = x._2._2._1.split(",")(4).toDouble;(dur,x._2._2._1) }
      .filter{x=> x._1>= 1}
      .map{x=> x._2}
    output
  }
  /**
    * 提取用户的旅游记录,规则：周末，停留时间大于6小时,离家超过10km,且不是工作地点
    *
    * @param  StayPointWithoutWork:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+ "," +longitude+ "," +latitude] ；
    * @param  HomeLoc:RDD[String] = RDD[usr + "," + cellid + "," +longitude+ "," +latitude]
    * @return  RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat+","+maxdis+","+dis]//所有
    */
  def travelDurOverSixHours(StayPointWithoutWork:RDD[String],HomeLoc:RDD[String]):RDD[String]={
    val sp = StayPointWithoutWork.map{x=> val line = x.split(",");(line(0),x)}
      .filter{x=> val day = x._1;isWeekday(day) == false}
      .map{x=> val line = x._2.split(",");(line(1),(x._2,(line(5),line(6))))}//(user,(info,(lng,lat)))
    val home = HomeLoc.map{x=>val line = x.split(",");(line(0),(line(2),line(3)))}.distinct()
    val result = home.join(sp).map{x=> val distance = GetDistance(x._2._1,x._2._2._2);val dur = x._2._2._1.split(",")(4);(x._2._2._1,distance,dur)}.filter{x=> x._2 < 100000.0 && x._2 >= 10 && x._3.toDouble >=6}.map{x=> x._1}
    result


  }

  /**
    * 提取用户当前月的周末出行总时长,出行次数和目的地个数
    *
    * @param  TravelOutsideLifecircle:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+lng+","+lat+","+distance] ；
    * @return  RDD[(String,String)] = RDD[(user,(time,recnum,desnum))]//指定位置所有轨迹
    */
  def travelDurAtWeekend(TravelOutsideLifecircle:RDD[String]):RDD[(String,String)] ={
    val totalTime = TravelOutsideLifecircle.map{x=> x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (user,dur.toDouble)}}
      .reduceByKey(_+_)//user,dur
    val travelNum = TravelOutsideLifecircle.map{x=> x.split(",") match {
        case Array(day,user,time,cell,dur,lng,lat) => (user,1)
      }}
        .reduceByKey(_+_)//user,num
    val destinationNum = TravelOutsideLifecircle.map{x=> x.split(",") match{
        case Array(day,user,time,cell,dur,lng,lat) => (user,cell)
      }}
        .distinct()
        .map{x=> (x._1,1)}
        .reduceByKey(_+_)
    val result = totalTime.join(travelNum)
      .map{x=>val user = x._1
        val dur = x._2._1
        val recNum = x._2._2
      (user,dur+","+recNum)}
      .join(destinationNum)
      .map{x=> val user = x._1
        val forwardFeat = x._2._1
        val desNum = x._2._2
        (user,forwardFeat+","+desNum)}
    result

  }
  /**
    * 提取用户当前月的工作日除家和工作地点以外的最常访问的top3第三地
    *
    * @param  StayPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat] ；
    * @param  Work:RDD[String]  = RDD[user,cell,lng,lat,flag]
    * @param  Home:RDD[String]  = RDD[user,cell,lng,lat,flag]
    * @return  RDD[(String,String)] = RDD[(user,cellid,count)]//
    */
  def top3LocAtWeekday(StayPoint:RDD[String],Work:RDD[String],Home:RDD[String]):RDD[(String,String)] ={
    val home = Home.map{x=> val line = x.split(",");(line(0)+","+line(1),1)}.distinct()
    val work = Work.map{x=> val line = x.split(",");(line(0)+","+line(1),1)}.distinct()
    val sp = StayPoint.map{x=> val line = x.split(",");(line(0),line(1),line(3),line(4))}
      .filter{x=> val day = x._1
        val dur = x._4
        isWeekday(day) && dur.toDouble >= 1}
      .map{x=>
        val user = x._2
        val cell =x._3
        (user+","+cell,1)}
      .leftOuterJoin(home)
      .filter{x=> val flag = x._2._2.getOrElse(0); flag == 0}
      .map{x=> (x._1,1)}//(user+","+cell,1)
      .leftOuterJoin(work)
      .filter{x=> val flag = x._2._2.getOrElse(0); flag == 0}
      .map{x=> (x._1,1)}//(user+","+cell,1)
      .reduceByKey(_+_)//每个地点去过几次
      .map{x=>
        val user = x._1.split(",")(0)
        val cell = x._1.split(",")(1)
      (user,(cell,x._2))
    }.groupByKey().map{x=> val arr = x._2.toArray.sortWith((a,b) => a._2 > b._2)
      if(arr.length >= 3){
        (x._1,arr(0)._1+","+arr(0)._2+","+arr(1)._1+","+arr(1)._2+","+arr(2)._1+","+arr(2)._2)
      }else if(arr.length == 2){
        (x._1,arr(0)._1+","+arr(0)._2+","+arr(1)._1+","+arr(1)._2)
      }else{
        (x._1,arr(0)._1+","+arr(0)._2)

      }


    }
    sp
  }


  /**
    * 提取用户当前月的周末除家和工作地点以外的最常访问的top3第三地
    *
    * @param  StayPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat] ；
    * @param  Work:RDD[String]  = RDD[user,cell,lng,lat,flag]
    * @param  Home:RDD[String]  = RDD[user,cell,lng,lat,flag]
    * @return  RDD[(String,String)] = RDD[(user,cellid+count)]
    */
  def top3LocAtWeekend(StayPoint:RDD[String],Work:RDD[String],Home:RDD[String]):RDD[(String,String)] ={
    val home = Home.map{x=> val line = x.split(",");(line(0)+","+line(1),1)}.distinct()
    val work = Work.map{x=> val line = x.split(",");(line(0)+","+line(1),1)}.distinct()
    val sp = StayPoint.map{x=> val line = x.split(",");(line(0),line(1),line(3),line(4))}
      .filter{x=> val day = x._1
        val dur = x._4
        isWeekday(day) == false && dur.toDouble >= 1}
      .map{x=>
        val user = x._2
        val cell =x._3
        (user+","+cell,1)}
      .leftOuterJoin(home)
      .filter{x=> val flag = x._2._2.getOrElse(0); flag == 0}
      .map{x=> (x._1,1)}//(user+","+cell,1)
      .leftOuterJoin(work)
      .filter{x=> val flag = x._2._2.getOrElse(0); flag == 0}
      .map{x=> (x._1,1)}//(user+","+cell,1)
      .reduceByKey(_+_)//每个地点去过几次
      .map{x=>
      val user = x._1.split(",")(0)
      val cell = x._1.split(",")(1)
      (user,(cell,x._2))
    }.groupByKey().map{x=> val arr = x._2.toArray.sortWith((a,b) => a._2 > b._2)
      if(arr.length >= 3){
        (x._1,arr(0)._1+","+arr(0)._2+","+arr(1)._1+","+arr(1)._2+","+arr(2)._1+","+arr(2)._2)
      }else if(arr.length == 2){
        (x._1,arr(0)._1+","+arr(0)._2+","+arr(1)._1+","+arr(1)._2)
      }else{
        (x._1,arr(0)._1+","+arr(0)._2)

      }


    }
    sp
  }
}
