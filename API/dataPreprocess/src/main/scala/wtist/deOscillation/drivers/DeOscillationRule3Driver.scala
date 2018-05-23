package wtist.deOscillation.drivers

import org.apache.spark.rdd.RDD
import wtist.{Env,Tools}

import wtist.deOscillation.impl.DeOscillationRule3._
import Tools._

/**
 * Created by wtist on 2016/6/28.
 */
object DeOscillationRule3Driver {
  val usage =
    """
    Usage: DeOscillationRule3Driver inputDir outputDir speedThreshold disThreshold
    """

  def main(args: Array[String]): Unit = {
    Env.check(args, usage, true, true)
    val sc = Env.init(args, "DeOscillationRule3")
    val inputDir = args(0)
    val outputDir = args(1)
    val speedThreshold = args(2).toLong
    val disThreshold = args(3).toLong
    val inputRdd = sc.textFile(inputDir)
    println("speed_threshold: " + speedThreshold + " distance_threshold: " + disThreshold)
    println("input: " + inputDir + " output: " + outputDir)
    val rule3 = driver(inputRdd, speedThreshold, disThreshold)
    rule3.saveAsTextFile(outputDir)
    sc.stop()
  }

  /**
   * 驱动函数
   * @param rdd：RDD[user + "," + province + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint]
   * @param speedThreshold: 250km/h Long
   * @param disThreshold: 100 km Long
   * @return RDD[user + "," + province + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint]
   */
  def driver(rdd: RDD[String], speedThreshold: Long = 250, disThreshold: Long = 100): RDD[String] = {
    val outputRdd = rdd.map(x => x.split(",") match {
      case Array(user, cell, lng, lat, first, last, isStablePoint) => (user, (cell, lng, lat, first, last, isStablePoint))
    }).groupByKey().map { x =>
      val loc = x._2.toArray.sortWith((a, b) => timetostamp(a._4).toLong < timetostamp(b._4).toLong)
      val rule_3 = OscillationRule3(loc, speedThreshold, disThreshold)
      val user = x._1
      (user, rule_3)
    }.flatMapValues(x => x).map(x => x match {
      case (user, (cell, lng, lat, first, last, isStablePoint)) => user + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint
    })
    outputRdd
  }
}
