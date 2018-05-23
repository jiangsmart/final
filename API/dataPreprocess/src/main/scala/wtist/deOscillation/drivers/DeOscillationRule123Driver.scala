package wtist.deOscillation.drivers

import org.apache.spark.rdd.RDD
import wtist.{Tools, Env}
import wtist.Tools._
import wtist.deOscillation.impl.DeOscillationRule1._
import wtist.deOscillation.impl.DeOscillationRule2._
import wtist.deOscillation.impl.DeOscillationRule3._

/**
 * Created by wtist on 2016/7/21.
 */
object DeOscillationRule123Driver {
  val usage =
    """
    Usage: DeOscillationRule123Driver inputDir outputDir rule1TimeThreshold rule2TimeThreshold rule2DistanceThreshold rule3SpeedThreshold rule3DistanceThreshold
    """

  def main(args: Array[String]): Unit = {
    Env.check(args, usage, false, true)
    val sc = Env.init(args, "DeOscillationRule1 Rule2 Rule3")
    val inputDir = args(0)
    val outputDir = args(1)
    println("input: " + inputDir + " output: " + outputDir)
    val rule1TimeThreshold = args(2).toLong
    println("rule1TimeThreshold: " + rule1TimeThreshold)
    val rule2TimeThreshold = args(3).toLong
    println("rule2TimeThreshold: " + rule2TimeThreshold)
    val rule2DistanceThreshold = args(4).toLong
    println("rule2DistanceThreshold: " + rule2DistanceThreshold)
    val rule3SpeedThreshold = args(5).toLong
    println("rule3SpeedThreshold: " + rule3SpeedThreshold)
    val rule3DistanceThreshold = args(6).toLong
    println("rule3DistanceThreshold: " + rule3DistanceThreshold)
    val inputRdd = sc.textFile(inputDir)
    val rule1 = Rule123driver(inputRdd, rule1TimeThreshold, rule2TimeThreshold, rule2DistanceThreshold, rule3SpeedThreshold, rule3DistanceThreshold)
    rule1.saveAsTextFile(outputDir)
    sc.stop()
  }

  /**
   * 驱动函数
   * @param rdd：RDD[user+","+time+","+cell+","+lng+","+lat+","+province]
   * @param rule1TimeThreshold: 15 Long
   * @param rule2TimeThreshold: 1 Long
   * @param rule2DistanceThreshold: 10 Long
   * @param rule3SpeedThreshold: 250 Long
   * @param rule3DistanceThreshold: 100 Long
   * @return RDD[user + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint]
   */
  def Rule123driver(rdd: RDD[String], rule1TimeThreshold: Long = 2, rule2TimeThreshold: Long = 1, rule2DistanceThreshold: Long = 10, rule3SpeedThreshold: Long = 250, rule3DistanceThreshold: Long = 100): RDD[String] = {
    val outputRdd = rdd.map(x => x.split(",") match {
      case Array(user, cell, lng, lat, first, last, isStablePoint) => (user , (cell, lng, lat, first, last, isStablePoint))
    }).groupByKey().map { x =>
      val loc = x._2.toArray.sortWith((a, b) => timetostamp(a._4).toLong < timetostamp(b._4).toLong)
      val rule_1 = OscillationRule1(loc, rule1TimeThreshold)
      val rule1_sort = rule_1.sortWith((a, b) => timetostamp(a._4).toLong < timetostamp(b._4).toLong)
      val rule_2 = OscillationRule2(rule1_sort,rule2TimeThreshold, rule2DistanceThreshold)
      val rule2_sort = rule_2.sortWith((a, b) => timetostamp(a._4).toLong < timetostamp(b._4).toLong)
      val rule_3 = OscillationRule3(rule2_sort,rule3SpeedThreshold, rule3DistanceThreshold)
      val rule3_sort = rule_3.sortWith((a, b) => timetostamp(a._4).toLong < timetostamp(b._4).toLong)
      val user = x._1
      (user, rule3_sort)
    }.flatMapValues(x => x).map(x => x match {
      case (user, (cell, lng, lat, first, last, isStablePoint)) => user + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint
    })
    outputRdd
  }
}
