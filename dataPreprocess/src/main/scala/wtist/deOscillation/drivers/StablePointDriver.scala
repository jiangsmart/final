package wtist.deOscillation.drivers

import org.apache.spark.rdd.RDD
import wtist.{Tools, Env}
import wtist.deOscillation.impl.StablePoint._
import Tools._


/**
 * Created by wtist on 2016/6/27.
 */
object StablePointDriver {
  val usage =
    """
    Usage: StablePointDriver inputDir outputDir timeThreshold
    """

  def main(args: Array[String]): Unit = {
    Env.check(args, usage, false, true)
    val sc = Env.init(args, "StablePoint")
    val inputDir = args(0)
    val outputDir = args(1)
    val timeThreshold = args(2).toLong
    println("time_threshold: " + timeThreshold)
    println("input: " + inputDir + " output: " + outputDir)
    val inputRdd = sc.textFile(inputDir)
    val stablePoint = driver(inputRdd, timeThreshold)
    stablePoint.saveAsTextFile(outputDir)
    sc.stop()
  }

  /**
   * 驱动函数
   * @param rdd：RDD[user+","+time+","+cell+","+lng+","+lat+","+province]
   * @param timeThreshold: 15 Long
   * @return RDD[user + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint]
   */
  def driver(rdd: RDD[String], timeThreshold: Long = 15): RDD[String] = {
    val outputRdd = rdd.map(x => x.split(",") match {
      case Array(user, time, cell, lng, lat, province) => (user + "," + province, (time, cell, lng, lat))
    }).groupByKey().map { x =>
      val loc = x._2.toArray.sortWith((a, b) => timetostamp(a._1).toLong < timetostamp(b._1).toLong)
      val stable = GetStablePoint(loc, timeThreshold)
      val user = x._1
      (user, stable)
    }.flatMapValues(x => x).map(x => x match {
      case (user, (cell, lng, lat, first, last, isStablePoint)) => user + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint
    })
    outputRdd
  }
}
