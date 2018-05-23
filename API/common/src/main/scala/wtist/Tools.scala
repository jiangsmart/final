package wtist

/**
  * Created by chenqingqing on 2016/3/10.
  */
import java.text.SimpleDateFormat
import java.util.{TimeZone, Calendar, Date}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by lingfeng
  * Date: 2015-07-16
  * Time: 14:56
  */

/**
  * 实验所用的一些工具类
  */
object Tools {
  /**
    * 转化为弧度(rad)
    */
  def Rad(d: Double): Double ={
    val rad = d * Math.PI / 180.0
    rad
  }
  /**
    * 基于googleMap中的算法得到两经纬度之间的距离,计算精度与谷歌地图的距离精度差不多，相差范围在0.2米以下
    *
    * @param lon1 第一点的经度
    * @param lat1 第一点的纬度
    * @param lat2 第二点的经度
    * @param lon2 第二点的纬度
    * @return 返回的距离，单位km
    */

  def GetDistance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val EARTH_RADIUS = 6371.004
    val radLat1 = Rad(lat1)
    val radLat2 = Rad(lat2)
    val radLon1 = Rad(lon1)
    val radLon2 = Rad(lon2)
    val a = radLat1 - radLat2
    val b = radLon1 - radLon2
    val s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2)+Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)))
    val distance = s * EARTH_RADIUS
    distance
  }

  /**
    * 对GetDistance(lon1: Double, lat1: Double, lon2: Double, lat2: Double)封装
    *
    * @param cella
    * @param cellb
    * @return
    */
  def GetDistance(cella: (String, String), cellb: (String, String)): Double = {
    val value = if((!(cella._1.equals("None")))&&(!(cellb._1.equals("None")))) {
      GetDistance(cella._1.toDouble, cella._2.toDouble, cellb._1.toDouble, cellb._2.toDouble)
    } else {
      100000.0
    }
    value
  }

  /**
   *  根据两个点以及各自的时间计算速度
    *
    * @param cella 第一位置点经纬度tuple(String, String)
   * @param cellb 第二位置点经纬度tuple(String, String)
   * @param timea 第一位置点时间 yyyyMMddHHmmss
   * @param timeb 第二位置点时间 yyyyMMddHHmmss
   * @return
   */
  def GetSpeed(cella: (String, String), cellb: (String, String), timea: String, timeb: String) :Double = {
    val distdiff = GetDistance(cella,cellb)
    val speed =
    if(distdiff == 100000.0) {
      0.0
    } else {
      val timediff = TimeDiff(timea, timeb)
      distdiff * 3600 / timediff
    }
    speed
  }

  /**
   *  根据距离及前后时间返回速度
    *
    * @param distdiff 距离，如果为默认距离，则速度返回0.0
   * @param timea 开始时间 yyyyMMddHHmmss
   * @param timeb 结束时间 yyyyMMddHHmmss
   * @return
   */
  def GetSpeed(distdiff: Double = 100000.0, timea: String, timeb: String) :Double = {
    val speed =
      if(distdiff == 100000.0) {
        0.0
      } else {
        val timediff = TimeDiff(timea, timeb)
        distdiff * 3600/ timediff
      }
    speed
  }


  /**
   * 计算两个点的距离
    *
    * @param cella tuple(lng,lat)
   * @param cellb tupe(lng, lat)
   * @return
   */
  def ComputeDistance(cella: (Double, Double), cellb: (Double, Double)): Double = {
    val value =
      GetDistance(cella._1, cella._2, cellb._1, cellb._2)
    value
  }

  /**
    *  将时间转成时间戳函数 秒数
    *
    * @param string_time 时间： yyyyMMddHHmmss
    * @return 时间戳 : String
    */
  def timetostamp(string_time: String) :String = {
    val sdf= new SimpleDateFormat("yyyyMMddHHmmss")
    val d = sdf.parse(string_time)
    String.valueOf(d.getTime()).substring(0, 10)
  }

  def time2stamp(string_time: String) :Long = {
    val sdf= new SimpleDateFormat("yyyyMMddHHmmss")
    val d = sdf.parse(string_time)
    d.getTime()
  }
  def getZeroTimeStamp(current: String): Long = {
     val zero = (time2stamp(current) / (1000 * 3600 * 24)  + 1)* (1000 * 3600 * 24) - TimeZone.getDefault.getRawOffset
    zero
  }

  def getHalfIndex(current: String): Long ={
    val result =(time2stamp(current) - getZeroTimeStamp(current)) / (1000*1800)
    result
  }
  /**
   *  时间差 返回秒 注意：开始时间 < 结束时间
    *
    * @param start_time 开始时间
   * @param end_time 结束时间
   * @return
   */
  def TimeDiff(start_time: String, end_time: String) :Long = {
      val start_stamp = timetostamp(start_time).toLong
      val end_stamp = timetostamp(end_time).toLong
      val diff = Math.abs(end_stamp - start_stamp)
      diff
  }



  /**
   * 时间转时间戳 返回到毫秒级别
    *
    * @param string_time 时间
   * @param format 时间格式
   * @return
   */
  def timetostamp(string_time: String, format: String) :String = {
    val sdf= new SimpleDateFormat(format)
    val d = sdf.parse(string_time)
    String.valueOf(d.getTime())
  }

  /**
   * 时间戳转时间 返回到毫秒级别
    *
    * @param stamp 时间戳
   * @param format 返回的时间格式
   * @return
   */
  def stamptotime(stamp: Long, format: String) : String = {
    val sdf = new SimpleDateFormat(format)
    val sd = sdf.format(new Date(stamp))
    sd
  }

  /**
   * 判断是否是工作日（周一至周五）
    *
    * @param currentDate
   * @return
   */
  def isWeekday(currentDate: String ): Boolean ={
    val df = new SimpleDateFormat("yyyyMMdd")
    val  d = df.parse(currentDate)
    val cal = Calendar.getInstance()
    cal.setTime(d)
    val w = cal.get(Calendar.DAY_OF_WEEK)
    return  w!=1 && w!=7
  }

  /**
   * 判断数字是否是奇数
    *
    * @param num 该数字
   * @return true(odd) or flase(even)
   */
  def isOdd(num: Int): Boolean ={
     val is_odd= num % 2 == 1
    is_odd
  }

  /**
   * 判断数字是否为0
    *
    * @param num 该数字
   * @return true(num == 0) or false
   */
  def isZero(num: Int): Boolean = {
    val is_zero = num == 0
    is_zero
  }

  /**
   * 返回两个Double型数据最大
    *
    * @param x
   * @param y
   * @return
   */
  def Max(x: Double, y: Double): Double={
    val result =
      if (x > y) {
        x
      } else {
        y
      }
    result
  }
  /**
   * 计算Poi总数
   **/
  def toSum(tmp:(Int,Int,Int,Int,Int,Int,Int,Int)):Int={
    tmp._1+tmp._2+tmp._3+tmp._4+tmp._5+tmp._6+tmp._7+tmp._8
  }

  /**
   * 计算某个停留点某类的POI分值
    *
    * @param sum
   * @param n_i
   * @param staycount
   * @param staycontainsi
   * @return
   */
  def TFIDF(sum:Int,n_i:Int,staycount:Int,staycontainsi:Int):Double={
    var scores =0.0
    if(sum!=0){
      if(staycontainsi!=0){
        scores= (n_i/sum)*Math.log(staycount/staycontainsi)
      }else{
        scores =(n_i/sum)*Math.log(staycount)
      }
    }else{
      scores=1.0
    }
    scores
  }
  /**
    * 得到指定日期之间的所有日期，结果中包括指定的日期
    *
    * @param startTime: String 开始的日期
    * @param endTime: String 结束的日期
    * @return Array[String] 指定日期之间的所有日期
    */
  def getDatesArray(startTime: String,endTime: String):Array[String] ={
    val startDay = Calendar.getInstance();
    val endDay = Calendar.getInstance();
    val df = new SimpleDateFormat("yyyyMMdd")
    startDay.setTime(df.parse(startTime))
    endDay.setTime(df.parse(endTime))
    if (startDay.compareTo(endDay) >= 0) {
      print("error:start time must be earlier than the end time!")
    }
    // 现在打印中的日期
    val currentPrintDay = startDay
    var flag = false
    val arrBuf = new ArrayBuffer[String]()
    while (!flag) {

      // 判断是否达到终了日，达到则终止打印
      if (currentPrintDay.compareTo(endDay) == 0) {
        flag = true
      }
      // 打印日期
      arrBuf += df.format(currentPrintDay.getTime())
      // 日期加一
      currentPrintDay.add(Calendar.DATE, 1)

    }
    arrBuf.toArray


  }
  /**
   * 已知索引值求地图方格的中心点坐标
   * @param index
   * @return
   */
  def indexToCenterCoordinate(index: Int): String = {
    //左下角经度
    val minLon: Double = 108.55
    //左下角纬度
    val minLat: Double = 18.1485
    //右上角经度
    val maxLon: Double = 111.069
    //右上角纬度
    val maxLat: Double = 20.1746
    val length = GetDistance(minLon, minLat, maxLon, minLat) //矩形的长266km
    val width = GetDistance(minLon, minLat, minLon, maxLat) //矩形的宽225km
    val numl = (length / 5).toInt //532
    val numw = (width / 5).toInt //450
    val x = index % numl - 0.5
    val y = index / numl + 0.5
    val lon = minLon + (maxLon - minLon) * x/numl
    val lat = minLat + (maxLat - minLat) * y/numw
    lon + "," + lat
  }

  /**
   * 为海南地图添加正方形索引，边长500m
   *
   * @param lon
   * @param lat
   * @return
   */
  def squareIndex(lon: Double, lat: Double): Int = {
    //左下角经度
    val minLon: Double = 108.55
    //左下角纬度
    val minLat: Double = 18.1485
    //右上角经度
    val maxLon: Double = 111.069
    //右上角纬度
    val maxLat: Double = 20.1746
    var index = 0
    if (lon < minLon || lon > maxLon || lat < minLat || lat > maxLat) {
      index = -1
    } else {
      val length = GetDistance(minLon, minLat, maxLon, minLat) //矩形的长266km
      val width = GetDistance(minLon, minLat, minLon, maxLat) //矩形的宽225km
      val numl = (length / 5).toInt //532
      val numw = (width / 5).toInt //450
      val length1 = GetDistance(minLon, minLat, lon, minLat) //矩形的长
      val width1 = GetDistance(minLon, minLat, minLon, lat) //矩形的宽
      val numl1 = (length1 / 5).toInt
      val numw1 = (width1 / 5).toInt
      index = numw1 * numl + numl1 + 1
    }
    index
  }

}

