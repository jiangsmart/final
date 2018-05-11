package wtist.deOscillation.drivers

import org.apache.spark.rdd.RDD
import wtist.{Tools,Env}
import wtist.deOscillation.impl.DeSameTimeRecord._
import wtist.deOscillation.impl.StablePoint._
import Tools._

/**
 * Created by wtist on 2016/7/5.
 */
object DeOscillationDeSameCellAndStablePoint {
  val usage =
    """
    Usage: DeOscillationDeSameCellAndStablePoint inputDir outputDir stableTimeThreshold
    """
  def main(args: Array[String]): Unit = {
    //Env.check(args, usage, false, true)
    val sc = Env.init(args, "DeSameCell Stable Point")
    val inputDir = args(0)
    val outputDir = args(1)
    println("input: " + inputDir + " output: " + outputDir)
    val stableTimeThreshold = args(2).toLong
    println("stableTimeThreshold: " + stableTimeThreshold)
    val inputRdd = sc.textFile(inputDir)
    val stablePoint = DeSameCellAndStablePointDriver(inputRdd, stableTimeThreshold)
    stablePoint.saveAsTextFile(outputDir)
    sc.stop()
  }

  /**
   * 驱动函数-去重复基站及静态点提取
   * @param rdd：RDD[user+","+time+","+cell+","+lng+","+lat+","]
   * @param timeThreshold: 15 Long
   * @return RDD[user + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint]
   */
  def DeSameCellAndStablePointDriver(rdd: RDD[String], timeThreshold: Long = 15): RDD[String] = {
    val outputRdd = rdd.map(x => x.split(",") match {
      case Array(user, time, cell, lng, lat) => (user , (time, cell, lng, lat))
    }).groupByKey().map { x =>
      val de_same_cell = DeSameTimeRecord(x._2.toArray)
      val stable = GetStablePoint(de_same_cell, timeThreshold)
      val stable_sort = stable.sortWith((a, b) => timetostamp(a._4).toLong < timetostamp(b._4).toLong)
      (x._1, stable_sort)
    }.flatMapValues(x => x).map(x => x match {
      case (user, (cell, lng, lat, first, last, isStablePoint)) => user + "," + cell + "," + lng + "," + lat + "," + first + "," + last + "," + isStablePoint
    })
    outputRdd
  }
}