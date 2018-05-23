package wtist.deOscillation.drivers

import org.apache.spark.rdd.RDD
import wtist.{Tools, Env}
import Tools._
import wtist.deOscillation.impl.DeOscillationRule2._

/**
 * Created by wtist on 2016/6/28.
 */
object DeOscillationRule2Driver {
  val usage =
    """
    Usage: DeOscillationRule2Driver inputDir outputDir timeThreshold disThreshold
    """

  def main(args: Array[String]): Unit = {
    Env.check(args, usage, true, true)
    val sc = Env.init(args, "DeOscillationRule2")
    val inputDir = args(0)
    val outputDir = args(1)
    println("input: " + inputDir + " output: " + outputDir)
    val timeThreshold = args(2).toLong
    val disThreshold = args(3).toLong
    println("time_threshold: " + timeThreshold + " distance_threshold: " + disThreshold)
    val inputRdd = sc.textFile(inputDir)
    val rule2 = driver(inputRdd, timeThreshold, disThreshold)
    rule2.saveAsTextFile(outputDir)
    sc.stop()
  }

  /**
   * 驱动函数
   * @param rdd：RDD[user + "," + province + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint]
   * @param timeThreshold: 1 min Long
   * @param disThreshold: 10 km Long
   * @return RDD[user + "," + province + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint]
   */
  def driver(rdd: RDD[String], timeThreshold: Long = 1, disThreshold: Long = 10): RDD[String] = {
    val outputRdd = rdd.map(x => x.split(",") match {
      case Array(user,  cell, lng, lat, first, last, isStablePoint) => (user , (cell, lng, lat, first, last, isStablePoint))
    }).groupByKey().map { x =>
      val loc = x._2.toArray.sortWith((a, b) => timetostamp(a._4).toLong < timetostamp(b._4).toLong)
      val rule_2 = OscillationRule2(loc,timeThreshold, disThreshold)
      val user = x._1
      (user, rule_2)
    }.flatMapValues(x => x).map(x => x match {
      case (user, (cell, lng, lat, first, last, isStablePoint)) => user + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint
    })
    outputRdd
  }
}
