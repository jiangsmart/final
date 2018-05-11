package wtist.userProfile.impl

import org.apache.spark.rdd.RDD
import wtist.Tools
import Tools._


/**
  * Created by chenqingqing on 2016/5/25.
  */
object HomeWorkProfile {
  /**
    * 得到每个用户的工作日平均离家最早时间,返回的是离00:00：00的小时数
    *
    * @param StayPoint:RDD[String] = day + user + time + cell + dur+ lng +lat
    * @return  RDD[String] = RDD[user,homeLeftTime]
    */
  def earlistHomeLeftTime(StayPoint:RDD[String],Work:RDD[String],Home:RDD[String]):RDD[(String,Double)] = {
    val weekdaySP = StayPoint.map { x => x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (user, (day, (time, cell)))
    }}
      .filter { x => val day = x._2._1;isWeekday(day)}
      .cache()
    val home = Home.map { x => val line = x.split(","); (line(0),line(1)) }.distinct()
    val filteredWeekdaySP = home.join(weekdaySP)
      .map{case (user, (home,(day, (time, cell))))=>
        (user+","+day+","+home,(time,cell))//user+day+home,time + cell
    }
      .groupByKey()
      .map{x=>
      val arr = x._2.toArray.sortWith((a,b)=>
        timetostamp(a._1,"yyyyMMddHHmmss").toLong < timetostamp(b._1,"yyyyMMddHHmmss").toLong)
      val day = x._1.split(",")(1)
      val home = x._1.split(",")(2)
      (x._1.split(",")(0),homeLeftTime(arr,home,day))//user,homeLeftTime
    }
      .filter{x=> x._2 != 0}
      .groupByKey()
      .map{x=> (x._1,x._2.reduce(_+_)/x._2.toArray.length)}
    filteredWeekdaySP
  }
  /**
    * 得到每个用户的工作日平均离开工作地点最晚时间,返回的是离这一天23:59:59的小时数
    *
    * @param StayPoint:RDD[String] = day + user + time + cell + dur+ lng +lat
    * @return  RDD[String] = RDD[user,workLeftTime]
    */

  def latesWorkLeftTime(StayPoint:RDD[String],Work:RDD[String]):RDD[(String,Double)] = {
    val weekdaySP = StayPoint.map { x => x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (user, (day, (time, cell)))
    }}
      .filter { x => val day = x._2._1;isWeekday(day) }
      .cache()
    val work = Work.map { x => val line = x.split(","); (line(0),line(1)) }.distinct()
    val filteredWeekdaySP = work.join(weekdaySP)
      .map{case (user, (home,(day, (time, cell))))=>
        (user+","+day+","+work,(time,cell))
      }
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray.sortWith((a,b)=>
          timetostamp(a._1,"yyyyMMddHHmmss").toLong < timetostamp(b._1,"yyyyMMddHHmmss").toLong)
        val day = x._1.split(",")(1)
        val work = x._1.split(",")(2)
        (x._1.split(",")(0),workLeftTime(arr,work,day))//user,workLeftTime
    }
      .filter{x=> x._2 != 0}
      .groupByKey()
      .map{x=> (x._1,x._2.reduce(_+_)/x._2.toArray.length)}
    filteredWeekdaySP
  }
  /**
    * 得到每个用户的工作日平均到达工作地点最早时间,返回的是离这一天00:00：00的小时数
    *
    * @param StayPoint:RDD[String] = day + user + time + cell + dur+ lng +lat
    * @param Work:RDD[String]
    * @return  RDD[String] = RDD[user,workArrivalTime]
    */
  def earlistWorkArrivalTime(StayPoint:RDD[String],Work:RDD[String]):RDD[(String,Double)] = {
    val weekdaySP = StayPoint.map { x => x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (user, (day, (time, cell)))
    }}
      .filter { x => val day= x._2._1;isWeekday(day) }
      .cache()
    val work = Work.map { x => val line = x.split(","); (line(0),line(1)) }
    val filteredWeekdaySP = work.join(weekdaySP)
      .map{case(user, (work,(day, (time, cell))))=>
        (user+","+day+","+work,(time,cell))
      }
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray.sortWith((a,b)=>
          timetostamp(a._1,"yyyyMMddHHmmss").toLong < timetostamp(b._1,"yyyyMMddHHmmss").toLong)
        val day = x._1.split(",")(1)
        val work = x._1.split(",")(2)
        (x._1.split(",")(0),workArrivalTime(arr,work,day))//user,workArrivalTime
      }
      .filter{x=> x._2 != 0}
      .groupByKey()
      .map{x=> (x._1,x._2.reduce(_+_)/x._2.toArray.length)}
    filteredWeekdaySP
  }
  /**
    * 得到每个用户的工作日平均下班后最早回家时间,返回的是离这一天23:59:59的小时数
    *
    * @param StayPoint:RDD[String] = day + user + time + cell + dur+ lng +lat
    * @param Work:RDD[String]
    * @param Home:RDD[String]
    * @return  RDD[String] = RDD[user,workArrivalTime]
    */
  def earlistHomeArrivalTime(StayPoint:RDD[String],LatesWorkLeftTime:RDD[(String,String)],Work:RDD[String],Home:RDD[String]):RDD[(String,Double)]  ={
    val weekdaySP = StayPoint.map { x => x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (user, (day, (time, cell)))
    }}
      .filter { x => val day = x._2._1;isWeekday(day) }
      .cache()
    val home = Home.map { x => val line = x.split(","); (line(0),line(1)) }
    val filteredWeekdaySP = home.join(weekdaySP)
      .map{case(user, (home,(day, (time, cell))))=>
        (user,(day+","+home,(time,cell)))
      }
      .join(LatesWorkLeftTime)
      .map{x=> (x._1+","+x._2._1._1+","+x._2._2,x._2._1._2)}
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray.sortWith((a,b)=>
          timetostamp(a._1,"yyyyMMddHHmmss").toLong < timetostamp(b._1,"yyyyMMddHHmmss").toLong)
        val day = x._1.split(",")(1)
        val home = x._1.split(",")(2)
        val latesWorkLeftTime = x._1.split(",")(3).toDouble
        (x._1.split(",")(0),homeArrivalTime(arr,home,day,latesWorkLeftTime))//user,workArrivalTime
      }.groupByKey()
      .map{x=> (x._1,x._2.reduce(_+_)/x._2.toArray.length)}
    filteredWeekdaySP





  }
  /**
    * 得到每个用户工作日平均每天工作的时长
    *
    * @param StayPoint:RDD[String] = day + user + time + cell + dur+ lng +lat
    * @param Work:RDD[String]
    * @return  RDD[String] = RDD[(user,workdur)]
    */
  def aveWorkDurAtWeekday(StayPoint:RDD[String],Work:RDD[String]):RDD[(String,Double)] ={
    val weekdaySP = StayPoint.map {x=> x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,cell,dur)}}
      .filter { x => val day = x._1;isWeekday(day)}
    val work = Work.map {x => val line = x.split(","); (line(0)+","+line(1), 1)}//(user+work,1)
    val result = weekdaySP.map { x => (x._2 + "," + x._3, x) }
        .join(work)
        .map{x=>
          val day = x._2._1._1
          val user = x._2._1._2
          val dur = x._2._1._4.toDouble
          (day+","+user,dur)}
        .reduceByKey(_+_)
        .map{x=>
        val user = x._1.split(",")(1)
        (user,x._2)}
        .groupByKey()
        .map{x=> (x._1,x._2.reduce(_+_)/x._2.toArray.length)}
    result


  }
  /**
    * 得到每个用户周末平均每天工作的时长
    *
    * @param StayPoint:RDD[String] = day + user + time + cell + dur+ lng +lat
    * @param Work:RDD[String]
    * @return  RDD[String] = RDD[(user,workdur)]
    */
  def aveWorkDurAtWeekend(StayPoint:RDD[String],Work:RDD[String]):RDD[(String,Double)] ={
    val weekdaySP = StayPoint.map {x=> x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,cell,dur)}}
      .filter { x => val day = x._1;isWeekday(day) == false}
    val work = Work.map { x => val line = x.split(","); (line(0)+","+line(1), 1) }//(user+work,1)
    val result = weekdaySP.map { x => (x._2 + "," + x._3, x) }
        .join(work)
        .map{x=>
          val day = x._2._1._1
          val user = x._2._1._2
          val dur = x._2._1._4.toDouble
          (day+","+user,dur)}
        .reduceByKey(_+_)
        .map{x=>
        val user = x._1.split(",")(1)
        (user,x._2)
      }.groupByKey().map{x=> (x._1,x._2.reduce(_+_)/x._2.toArray.length)}
    result


  }
  /**
    * 工作圈到达工作地点时间排名,返回的是在用户的工作圈中比用户来得早的人的人数占工作圈总人数的比例
    *
    * @param Colleague:RDD[(String,String)] = user1,user2
    * @param EarlistWorkArrivalTime:RDD[(String,String)] = user,time
    * @return  RDD[String] = RDD[(user,portion)]
    */
  def WorkLocArrivalRank(Colleague:RDD[(String,String)],EarlistWorkArrivalTime:RDD[(String,String)]):RDD[(String,Double)]={
    //val Colleague = sc.textFile("/user/chenqingqing/social/colleague")
    val time = EarlistWorkArrivalTime.map{x=> (x._1,x._2.toDouble)}
    val colleague = Colleague.join(time)
      .map{case (user,(colleague,time1)) =>
        (colleague,(user,time1))}
      .repartition(500)
      .join(time)
        .map{case (colleague,((user,time1),time2)) => (user,(time1,time2))}
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray
        var count:Double = 0
        for(i<- 0 until arr.length){
          val time1 = arr(i)._1
          val time2 = arr(i)._2
          if( time2 < time1 ){
            count += 1
        }
      }
      (x._1,count/arr.length)

    }
    colleague


  }
  /**
    * 工作圈工作日在工作地点时长排名,返回的是在用户的工作圈中比用户呆的更久的人的人数占工作圈总人数的比例
    *
    * @param Colleague:RDD[(String,String)] = user1,user2
    * @param AveWorkDurAtWeekday:RDD[(String,String)] = user,time
    * @return  RDD[String] = RDD[(user,portion)]
    */
  def aveWorkDurAtWeekdayRank(Colleague:RDD[(String,String)],AveWorkDurAtWeekday:RDD[(String,String)]):RDD[(String,Double)]={
    val dur = AveWorkDurAtWeekday.map{x=> (x._1,x._2.toDouble)}
    val colleague = Colleague.join(dur)
      .map{case (user,(colleague,dur1)) =>
        (colleague,(user,dur1))}
      .repartition(500)
      .join(dur)
      .map{case (colleague,((user,dur1),dur2)) => (user,(dur1,dur2))}
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray
        var count:Double = 0
        for(i<- 0 until arr.length){
          val dur1 = arr(i)._1
          val dur2 = arr(i)._2
          if( dur2 > dur1 ){
            count += 1
          }
        }
        (x._1,count/arr.length)

      }
    colleague


  }
  /**
    * 工作圈周末在工作地点时长排名,返回的是在用户的工作圈中比用户呆的更久的人的人数占工作圈总人数的比例
    *
    * @param Colleague:RDD[String] = user1,user2
    * @param AveWorkDurAtWeekend:RDD[(String,String)] = user,time
    * @return  RDD[String] = RDD[(user,portion)]
    */
  def aveWorkDurAtWeekendRank(Colleague:RDD[(String,String)],AveWorkDurAtWeekend:RDD[(String,String)]):RDD[(String,Double)]={
    val dur = AveWorkDurAtWeekend.map{x=> (x._1,x._2.toDouble)}
    val colleague = Colleague.join(dur)
      .map{case (user,(colleague,dur1)) =>
        (colleague,(user,dur1))}
      .repartition(500)
      .join(dur)
      .map{case (colleague,((user,dur1),dur2)) => (user,(dur1,dur2))}
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray
        var count:Double = 0
        for(i<- 0 until arr.length){
          val dur1 = arr(i)._1
          val dur2 = arr(i)._2
          if( dur2 > dur1 ){
            count += 1
          }
        }
        (x._1,count/arr.length)

      }
    colleague


  }

  /**
    * 提取用户当前月的周末在家总时长,按月求和
    *
    * @param  StayPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat] ；
    * @param  HomeLngLat:RDD[String] = RDD[user,cell,lng,lat,flag]
    * @return  RDD[String] = RDD[(user,stayHomeDur)]
    */
  def totalHomeStayDurAtWeekend(StayPoint:RDD[String],HomeLngLat:RDD[String]):RDD[(String,Double)] ={
    val sp = StayPoint.map{x=> x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,cell,dur)
    }}
      .filter{x=> val day = x._1;isWeekday(day)==false }
      .map{case (day,user,cell,dur) => (user,(cell,dur))}
    val home = HomeLngLat.map{x=> val line =x.split(",");(line(0),line(1))}.distinct()
    val result = sp.join(home)
      .filter{x=>
        val cell =x._2._1._1
        val home = x._2._2
        cell.equals(home)}
      .map{x=>
        val user = x._1
        val dur = x._2._1._2.toDouble
        (user,dur)}
      .reduceByKey(_+_)
    result
  }

  /**
    * 提取用户当前月的周末平均每天在家的时长
    *
    * @param  StayPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat] ；
    * @param  HomeLngLat:RDD[String] = RDD[user,cell,lng,lat,flag]
    * @return  RDD[String] = RDD[user,aveStayHomeDur]
    */

  def aveHomeStayDurAtWeekend(StayPoint:RDD[String],HomeLngLat:RDD[String]):RDD[(String,Double)] = {
    val sp = StayPoint.map{x=> x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,cell,dur)
    }}
      .filter { x => val day = x._1;isWeekday(day) == false}
      .map { case (day,user,cell,dur) =>
      (user,(day,cell,dur))//user,(day,cell,dur)
    }
    val home = HomeLngLat.map { x => val line = x.split(","); (line(0), line(1)) }.distinct()
    val result = sp.join(home)
      .filter { x =>
        val cell = x._2._1._2
        val home = x._2._2
        cell.equals(home)}
      .map { case(user,((day,cell,dur),home)) =>
        (user + "," + day, dur.toDouble) }
      .groupByKey()
      .map { x =>
        val total = x._2.reduce(_+_)
        (x._1, total)}
      .map { x => val user = x._1.split(",")(0); (user, x._2) }
      .groupByKey()
      .map { x =>
        val total = x._2.reduce(_ + _)
        val len = x._2.toArray.length
        val ave = total / len
        (x._1, ave)
      }
    result
  }
  /**
    * 提取用户当前月的工作日平均每天在家的时长
    *
    * @param  StayPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat] ；
    * @param  HomeLngLat:RDD[String] = RDD[user,cell,lng,lat,flag]
    * @return  RDD[String] = RDD[user,aveStayHomeDur]
    */

  def aveHomeStayDurAtWeekday(StayPoint:RDD[String],HomeLngLat:RDD[String]):RDD[(String,Double)] = {
    val sp = StayPoint.map{x=> x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,cell,dur)
    }}
      .filter { x => val day = x._1;isWeekday(day)}
      .map { case (day,user,cell,dur) =>
        (user,(day,cell,dur))//user,(day,cell,dur)
      }
    val home = HomeLngLat.map { x => val line = x.split(","); (line(0), line(1)) }.distinct()
    val result = sp.join(home)
      .filter { x =>
        val cell = x._2._1._2
        val home = x._2._2
        cell.equals(home)}
      .map { case(user,((day,cell,dur),home)) =>
        (user + "," + day, dur.toDouble) }
      .groupByKey()
      .map { x =>
        val total = x._2.reduce(_+_)
        (x._1, total)}
      .map { x => val user = x._1.split(",")(0); (user, x._2) }
      .groupByKey()
      .map { x =>
        val total = x._2.reduce(_ + _)
        val len = x._2.toArray.length
        val ave = total / len
        (x._1, ave)
      }
    result
  }
  /**
    * 返回用户每天离家最早的时间
    * 首先找到当日用户首次出现的家的记录，然后计算离家时间，如果离家时间早于5：00，则认为是异常；继续寻找家的位置记录，计算离家时间。重复以上步骤
    *
    * @param arr:Array[(String,String)] = time,cellId
    * @param Home:String 用户的家的位置
    * @param day:String 当日的日期，201508xx
    * @return Long:当前时间换算得到的小时数
    *
    */
  def homeLeftTime(arr:Array[(String,String)],Home:String,day:String): Double ={
    var i = 0;var flag =false;var home = 0;var leftTime:Long = 0;var result:Double = 0
    while(i<arr.length && !flag ){
      if(arr(i)._2.equals(Home) == true && home == 0){
        home = 1

      }
      if(arr(i)._2.equals(Home) == false && home == 1){
        leftTime = (timetostamp(arr(i)._1,"yyyyMMddHHmmss").toLong + timetostamp(arr(i-1)._1,"yyyyMMddHHmmss").toLong)/2
        home = 0
        if (stamptotime(leftTime,"yyyyMMddHHmmss").substring(8,10).toInt >= 4){
          flag = true
        }

      }

      i = i  + 1
    }
    if(flag){
      result = (leftTime - timetostamp(day+"000000","yyyyMMddHHmmss").toLong)/3600000.0
    }

    result





  }

  /**
    * 返回用户每天离开工作地点最晚的时间
    * 遍历用户得到当日time_sorted记录中最后一次在工作地点出现的记录，然后计算和后一个记录的中间时间作为离开工作地点的时间
    *
    * @param arr:Array[(String,String)] = time,cellId
    * @param Work:String 用户的家的位置
    * @return Long:当前时间距离当日235959的小时数
    *
    *
    */
  def workLeftTime(arr:Array[(String,String)],Work:String,day:String): Double ={
    var restore = "";var pointer = 0;var result:Double = 0;var leftTime:Long = 0
    for (i<- 0 until arr.length){
      if(arr(i)._2.equals(Work)){
        restore = arr(i)._1
        pointer = i

      }
    }
    if(restore.equals("") == false){
      if(pointer == (arr.length-1)){
        leftTime = timetostamp(arr(pointer)._1,"yyyyMMddHHmmss").toLong


      }else{
        leftTime  = (timetostamp(arr(pointer)._1,"yyyyMMddHHmmss").toLong + timetostamp(arr(pointer+1)._1,"yyyyMMddHHmmss").toLong)/2

      }
      result =  (timetostamp(day+"235959","yyyyMMddHHmmss").toLong - leftTime)/3600000.0


    }

    result

  }
  /**
    * 返回用户每天到达工作地点的最早时间
    * 遍历用户得到当日time_sorted记录，然后返回第一次匹配工作地点的记录
    *
    * @param arr:Array[(String,String)] = time,cellId
    * @param Work:String 用户的工作地点的位置
    * @return Long:当前时间距离当日00：00:00的小时数
    *
    *
    */
  def workArrivalTime(arr:Array[(String,String)],Work:String,day:String):Double ={
    var i = 0;var flag = false;var restore = "";var result:Double = 0;var arriveTime:Long = 0
    while(i<arr.length && !flag){
      if(arr(i)._2.equals(Work)){
        flag = true
        restore = arr(i)._1

      }
      i = i+1
    }
    if(flag){
      if(i == 1){
        arriveTime = timetostamp(arr(i-1)._1,"yyyyMMddHHmmss").toLong

      }else{
        arriveTime = (timetostamp(arr(i-1)._1,"yyyyMMddHHmmss").toLong + timetostamp(arr(i-2)._1,"yyyyMMddHHmmss").toLong)/2

      }
      result = (arriveTime - timetostamp(day+"000000","yyyyMMddHHmmss").toLong)/3600000.0


    }
    result




  }
  /**
    * 返回用户每天回家的最早时间，
    * 遍历用户得到当日time_sorted记录，从用户最晚从工作地点离开的时间以后找到的最早的家的记录和上一个位置取时间中间值
    *
    * @param arr:Array[(String,String)] = time,cellId
    * @param Home:String 用户的家的位置
    * @param day:String
    * @param latesWorkLeftTime:Double 由先前计算得到的
    * @return Long:当前时间距离当日23：59:59的小时数
    *
    *
    */
  def homeArrivalTime(arr:Array[(String,String)],Home:String,day:String,latesWorkLeftTime:Double):Double ={
    var i = 0;var flag = false;var baseline  = timetostamp(day+"235959","yyyyMMddHHmmss").toLong;var time :Double= 0;

    while( i< arr.length && !flag){
      if(arr(i)._2.equals(Home)){
        time = (baseline -timetostamp(arr(i)._1,"yyyyMMddHHmmss").toLong)/3600000.0
        if(time < latesWorkLeftTime){
          flag = true
        }

      }
      i += 1


    }
    if(!flag){time = 0}
    time

  }


}
