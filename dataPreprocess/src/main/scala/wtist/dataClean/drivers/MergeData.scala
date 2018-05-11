package wtist.dataClean.drivers

import java.io.File

import org.apache.spark.rdd.RDD
import wtist.{Env, Arguments}
import wtist.dataClean.impl.MergeData._

/**
 * 合并SM/CC/MM文件
 * Created by wtist on 2016/5/25.
 */
object MergeData {
  val usage = """
    Usage: MergeData inputDir outputDir month province abNormalUserDir
              """
  def main(args:Array[String]): Unit = {
   // Env.check(args, usage, false, true)
    val sc = Env.init(args, "MergeData")
    val inputDir = args(0)
    val outputDir = args(1)
    println("input: " + inputDir + " output: " + outputDir)

    val abNormalUserDir = args(4)
    println("abNormalUserDir: " + abNormalUserDir)



    val cc_abNormalUserDir = abNormalUserDir + File.separator + "CCAbNormalUser"
    val sm_abNormalUserDir = abNormalUserDir + File.separator + "SMAbNormalUser"
    val mm_abNormalUserDir = abNormalUserDir + File.separator + "MMAbNormalUser"

    println("cc_abNormalUserDir: " + cc_abNormalUserDir)
    val cc_userCleaned = sc.textFile(cc_abNormalUserDir)
    println("sm_abNormalUserDir: " + sm_abNormalUserDir)
    val sm_userCleaned = sc.textFile(sm_abNormalUserDir)
    println("mm_abNormalUserDir: " + mm_abNormalUserDir)
    val mm_userCleaned = sc.textFile(mm_abNormalUserDir)
    val userCleaned = (cc_userCleaned ++ sm_userCleaned ++ mm_userCleaned).distinct.map(x => x.split(",") match {case Array(user, province) => (user, 1)}).collectAsMap
    val broadCastAbnormalUserMap = sc.broadcast(userCleaned)

    val cc_inputDir = inputDir + File.separator + "CCNormalUserLoc.csv"
    println("cc_inputDir: " + cc_inputDir)
    val cc_inputRDD = sc.textFile(cc_inputDir)
    val sm_inputDir = inputDir + File.separator + "SMNormalUserLoc.csv"
    println("sm_inputDir: " + sm_inputDir)
    val sm_inputRDD = sc.textFile(sm_inputDir)
    val mm_inputDir = inputDir + File.separator + "MMNormalUserLoc.csv"
    println("sm_inputDir: " + mm_inputDir)
    val mm_inputRDD = sc.textFile(mm_inputDir)
    val mergeData = cc_inputRDD++sm_inputRDD++mm_inputRDD
    val outputMergeFile = outputDir + File.separator + "mergeDistinctNormalUserLoc.csv"
    Merge(mergeData, broadCastAbnormalUserMap).saveAsTextFile(outputMergeFile)

    sc.stop()
  }
}
