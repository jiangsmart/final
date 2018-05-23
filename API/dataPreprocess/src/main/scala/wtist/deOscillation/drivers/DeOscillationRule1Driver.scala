package wtist.deOscillation.drivers

import org.apache.spark.rdd.RDD
import wtist.{Tools, Env}
import wtist.Tools._
import wtist.deOscillation.impl.DeOscillationRule1._


/**
 * Created by wtist on 2016/6/28.
 */
object DeOscillationRule1Driver {
  val usage =
    """
    Usage: DeOscillationRule1Driver inputDir outputDir timeThreshold
    """

  def main(args: Array[String]): Unit = {
    Env.check(args, usage, false, true)
    val sc = Env.init(args, "DeOscillationRule1")
    val inputDir = args(0)
    val outputDir = args(1)
    println("input: " + inputDir + " output: " + outputDir)
    val timeThreshold = args(2).toLong
    println("time_threshold: " + timeThreshold)
    val inputRdd = sc.textFile(inputDir)
    val rule1 = driver(inputRdd, timeThreshold)
    rule1.saveAsTextFile(outputDir)
    sc.stop()
  }

  /**
   * 驱动函数
   * @param rdd：RDD[user+","+time+","+cell+","+lng+","+lat+","+province]
   * @param timeThreshold: 15 Long
   * @return RDD[user + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint]
   */
  def driver(rdd: RDD[String], timeThreshold: Long = 2): RDD[String] = {
    val outputRdd = rdd.map(x => x.split(",") match {
      case Array(user, cell, lng, lat, first, last, isStablePoint) => (user, (cell, lng, lat, first, last, isStablePoint))
    }).groupByKey().map { x =>
      val loc = x._2.toArray.sortWith((a, b) => timetostamp(a._4).toLong < timetostamp(b._4).toLong)
      val rule_1 = OscillationRule1(loc, timeThreshold)
      val user = x._1
      (user, rule_1)
    }.flatMapValues(x => x).map(x => x match {
      case (user, (cell, lng, lat, first, last, isStablePoint)) => user + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint
    })
    outputRdd
  }
}
