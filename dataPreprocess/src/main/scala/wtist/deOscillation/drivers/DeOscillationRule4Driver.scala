package wtist.deOscillation.drivers

import org.apache.spark.rdd.RDD
import wtist.{Tools, Env}
import wtist.deOscillation.impl.DeOscillationRule4._
import wtist.deOscillation.impl.GetLastTime._
import Tools._

/**
 * Created by wtist on 2016/6/28.
 */
object DeOscillationRule4Driver {
  val usage =
    """
    Usage: DeOscillationRule4Driver inputDir outputDir rule4TimeWindows rule4CountThreshold rule4UniqCountThreshold
    """

  def main(args: Array[String]): Unit = {
    Env.check(args, usage, false, true)
    val sc = Env.init(args, "DeOscillationRule4")
    val inputDir = args(0)
    val outputDir = args(1)
    println("input: " + inputDir + " output: " + outputDir)
    val rule4TimeWindows = args(2).toLong
    println("rule4TimeWindows: " + rule4TimeWindows)
    val rule4CountThreshold = args(3).toInt
    println("rule4CountThreshold: " + rule4CountThreshold)
    val rule4UniqCountThreshold = args(4).toInt
    println("rule4UniqCountThreshold: " + rule4UniqCountThreshold)
    val inputRdd = sc.textFile(inputDir)
    val rule4 = driver(inputRdd, rule4TimeWindows, rule4CountThreshold, rule4UniqCountThreshold)
    rule4.saveAsTextFile(outputDir)
    sc.stop()
  }

  /**
   * 驱动函数
   * @param rdd：RDD[user + "," + province + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint]
   * @param timeWindows 时间窗口 eg. 60秒
   * @param countThreshold 该窗口记录数
   * @param uniqCountThreshold 该窗口含有cell的个数
   * @return RDD[user + "," + province + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint]
   */
  def driver(rdd: RDD[String], timeWindows: Long, countThreshold: Int, uniqCountThreshold: Int): RDD[String] = {
    val outputRdd = rdd.map(x => x.split(",") match {
      case Array(user, cell, lng, lat, first, last, isStablePoint) => (user , (cell, lng, lat, first, last, isStablePoint))
    }).groupByKey().map { x =>
      val loc = x._2.toArray.sortWith((a, b) => timetostamp(a._4).toLong < timetostamp(b._4).toLong)
      val rule_4 = OscillationRule4(loc, timeWindows, countThreshold, uniqCountThreshold)
      val getlast = GetLastTime(loc) //对于last时间为“None”特殊处理
      val result = getlast.sortWith((a, b) => timetostamp(a._4).toLong < timetostamp(b._4).toLong)
      val user = x._1
      (user, result)
    }.flatMapValues(x => x).map(x => x match {
      case (user, (cell, lng, lat, first, last, isStablePoint)) => user + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint
    })
    outputRdd
  }
}
