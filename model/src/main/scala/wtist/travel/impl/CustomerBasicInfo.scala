package wtist.travel.impl

import org.apache.spark.rdd.RDD
import wtist.Tools
import Tools._


import scala.collection.mutable.ArrayBuffer
/**
  * Created by chenqingqing on 2016/6/1.
  */
object CustomerBasicInfo {
  /**
    * 识别游客,在岛内停留3-15天
    *
    * @param OtherProvRec:RDD[String] = user +","+ time +","+ cell+","+lng +","+ lat+","+prov
    * @return CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    */
  def customerDetect(OtherProvRec:RDD[String]):RDD[String]={
    val customerInfo = OtherProvRec.map{x=> x.split(",") match {
      case Array(user,time,cell,lng,lat,prov) => (user+"#"+prov,time)
    }}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b) =>
        timetostamp(a).toLong < timetostamp(b).toLong)
        val len = arr.length
        val firstDate = arr(0).substring(0,8)
        val lastDate = arr(len-1).substring(0,8)
        val days = lastDate.substring(6,8).toInt - firstDate.substring(6,8).toInt + 1
        (x._1,firstDate,lastDate,days)}
      .filter{x=> val days = x._4;days >= 3 && days <= 15}
      .map{case(userAndProv,firstDate,lastDate,days) =>
        userAndProv+","+firstDate+","+lastDate+","+days}
      .repartition(10)
    customerInfo

  }
  /**
    * 返回当前用户来琼方式
    *
    * @param gps:(String,String) 游客的最后一个在岛停留点的百度经纬度
    * @return Int:0表示自驾或游轮，1表示火车，2表示飞机，3表示无法识别
    *PS:0：自驾港口 南港售票处百度坐标 （110.165757,20.04579） 秀英港百度坐标 （110.466232,19.945878）
    *   1：飞机场 海口美兰国际机场百度坐标 （110.466232,19.945878） 三亚机场（109.414883,18.313457）
    *   2：海口站（110.168949,20.033696） 三亚站（109.499415,18.302055）
    *算法描述：最后一个停留点（0.5h）离这些位置哪个近，且距离必须小于2Km,如果到所有点的距离都大于2km就判定为无法识别
    */
  def TransportationDetect(gps:(String,String)):Int ={
    val xiuyingPort = ("110.293096","20.026518")
    val southPort = ("110.165718","20.04586")
    val haikouAirPort = ("110.466232","19.945878")
    val sanyaAirPort = ("109.414883","18.313457")
    val haikouStation = ("110.168949","20.033696")
    val sanyaStation = ("109.499415","18.302055")
    val disArrayBuffer = new ArrayBuffer[(Int,Double)]()
    val disToPort = if(GetDistance(gps,xiuyingPort)<=GetDistance(gps,southPort)) GetDistance(gps,xiuyingPort) else GetDistance(gps,southPort)
    val disToAirport = if(GetDistance(gps,haikouAirPort)<=GetDistance(gps,sanyaAirPort)) GetDistance(gps,haikouAirPort) else GetDistance(gps,sanyaAirPort)
    val disToRailwayStation = if(GetDistance(gps,haikouStation)<=GetDistance(gps,sanyaStation)) GetDistance(gps,haikouStation) else GetDistance(gps,sanyaStation)
    disArrayBuffer += Tuple2(0,disToPort)
    disArrayBuffer += Tuple2(1,disToAirport)
    disArrayBuffer += Tuple2(2,disToRailwayStation)
    val sortedArr = disArrayBuffer.toArray.sortWith((a,b) => a._2 < b._2)
    var res = 3
    if(sortedArr(0)._2 <=2){
      //如果距离最小的位置点的距离满足小于2km,就认定为相应的交通方式，否则认定为无法判断
      res = sortedArr(0)._1
    }
    res

  }
  /**
    * 计算外地游客的来琼方式
    * @param  OtherProvStopPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat] ；
    * @param  CustomerInfo:RDD[String]  = RDD[user+"," +prov+"," +firstDate+"," +lastDate+"," +stayDays]
    * @param cellTrans:RDD[String]
    * @return  RDD[(String,String)] = RDD[user+"," +transportation]
    */
  def transportation(OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],cellTrans:RDD[String]):RDD[String] ={
    val bd = cellTrans.map(_.split(","))
      .filter{x=> x.length ==14}
      .map{x=> val cell = x(3)
        val bdLng = x(12)
        val bdLat = x(13)
        (cell,(bdLng,bdLat))}//(cell,(bdlng,bdlat))
    val customer = CustomerInfo.map{x=> x.split(",") match
      {case Array(user,prov,firstDay,lastDay,days) =>(user,lastDay)}}
        .distinct
    val recAtLastDay = OtherProvStopPoint.map{x=> val line =x.split(",");(line(3),x)}//(cell,line)
      .join(bd)
      .map{x=>
        val line= x._2._1
        val user = line.split(",")(1)
        val time = line.split(",")(2)
        val dur = line.split(",")(4)
        val bdlnglat = x._2._2
        (user,(time,dur,bdlnglat))}
      .join(customer)
      .filter{x=>
        val day = x._2._1._1.substring(0,8)
        val lastDay = x._2._2
        day.equals(lastDay)
      }
      .map{case((user,((time,dur,(lng,lat)),lastDay))) => (user,(time,dur,(lng,lat)))}
    val transport = recAtLastDay.filter{x=> val dur = x._2._2;dur.toDouble >= 0.5}
      .map{case((user,(time,dur,(lng,lat)))) => (user,(time,(lng,lat)))}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=>
        timetostamp(a._1)>timetostamp(b._1))
        (x._1,TransportationDetect(arr(0)._2))
      }
    val completeSet = customer.leftOuterJoin(transport)
      .map{x=> (x._1,x._2._2.getOrElse(3))}
    val bySelfDrive = completeSet.filter{x=> x._2 == 0}
    val byAir = completeSet.filter{x=> x._2 == 1}
    val byTrain = completeSet.filter{x=> x._2 == 2}
    val unDetest = completeSet.filter{x=> x._2 == 3}
      .join(recAtLastDay)
      .map{case(user, (flag, (time,dur,(lng,lat)))) => (user,(time,(lng,lat)))}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=>
        timetostamp(a._1)>timetostamp(b._1))
        (x._1,TransportationDetect(arr(0)._2))
      }
    val finalSet = bySelfDrive.union(byAir)
      .union(byTrain)
      .union(unDetest)
    val result = customer.leftOuterJoin(finalSet)
      .map{x=> (x._1,x._2._2.getOrElse(3))}
      .map{x=> x._1 +","+x._2}

    result



  }
  /**
    * 识别游客住宿地点的基站并返回百度坐标,规则：00：:0-06：00时间段内停留时间最长的基站
    *
    * @param OtherProvStopPoint:RDD[String] = user +","+ time +","+ cell +","+ lng +","+ lat +","+ prov
    * @param cellTrans:RDD[String]
    * @param CustomerInfo:RDD[String]
    * @return CustomerHome:RDD[String]  = user+","+homeCell +"," + baiduLng +","+ baiduLat
    */
  def homePlaceForCustomer(OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],cellTrans:RDD[String]):RDD[String] ={
    val bd = cellTrans.map(_.split(","))
      .filter{x=> x.length ==14}
      .map{x=> val cell = x(3)
        val bdLng = x(12)
        val bdLat = x(13)
        (cell,(bdLng,bdLat))}//(cell,(bdlng,bdlat))
    val customer = CustomerInfo.map{x=> val line = x.split(",")
        val user = line(0)
        (user,1)}
    val result = OtherProvStopPoint.map{x=> val time = x.split(",")(2);(time,x)}
      .filter{x=> val hour = x._1.substring(8,10).toInt;hour >=0 && hour <=6}
      .map{x=> val user = x._2.split(",")(1)
        val cell = x._2.split(",")(3)
        val dur = x._2.split(",")(4)
        (user+","+cell,dur.toDouble)}
      .reduceByKey(_+_)
      .map{x=> val user = x._1.split(",")(0)
        val cell = x._1.split(",")(1)
        val dur = x._2
        (user,(cell,dur))}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=> a._2> b._2)
        (arr(0)._1,x._1)}
      .join(bd)
      .map{case(cell,(user,(bdLng,bdLat))) => (user,cell+","+bdLng+","+bdLat)}
      .join(customer)
      .map{case(user,(home,1)) => user+","+home}
    result
  }

}
