package wtist.dataClean.drivers

import java.io.File

import wtist.{Env, Arguments}
import wtist.dataClean.impl.DeCardUsers._
/**
 * 去除上网卡模块
 * Created by wtist on 2016/5/25.
 */
object DeCardUserds {
  val usage = """
    Usage: DeCardUserds inputDir outputDir month(201512) province trueUserDir StartTime EndTime dayThreshold hourThreshold
              """
  def main(args:Array[String]): Unit = {
   // Env.check(args, usage, true, true)
    val sc = Env.init(args, "DeCardUserds")
    val inputDir = args(0)
    val outputDir = args(1)
    val month = args(2)
    println("input: " + inputDir + " output: " + outputDir)
    val trueUserDir = args(4)
    val StartTime = args(5)
    val EndTime = args(6)
    println("StartTime: " + StartTime + " EndTime: " + EndTime)

    val dayThreshold = args(7).toInt
    val hourThreshold = args(8).toInt
    println("dayThreshold: " + dayThreshold + " hourThreshold: " + hourThreshold)


    val trueUserDirOutput = trueUserDir + File.separator + month+"TrueUserLocal.csv"
    println("trueUserDirOutput: " + trueUserDirOutput)
    val totalUserLocInput = inputDir + File.separator + month+"TotalLocal.csv"
    println("totalUserLocInput: " + totalUserLocInput)
    val trueUserLocOutput = outputDir + File.separator + month+"TrueLocal.csv"
    println("trueUserLocOutput: " + trueUserLocOutput)


    val totalUserLoc = sc.textFile(totalUserLocInput)

    val trueUser = DeCardUser(totalUserLoc, StartTime, EndTime, dayThreshold, hourThreshold)
    println("save trueUser")
    trueUser.saveAsTextFile(trueUserDirOutput)
    val broadcastTrueUserMap = sc.broadcast(trueUser.map(x => x match {case user => (user, 1)}).collectAsMap())
    println("get trueUser record and save")
    GetSelectedUserLoc(totalUserLoc, broadcastTrueUserMap).saveAsTextFile(trueUserLocOutput)
    sc.stop()
  }
}
