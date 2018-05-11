package wtist.dataClean.drivers

import java.io.File

import wtist.Env
import wtist.dataClean.impl.DeAbnormalUsers._
/**
 * 去异常用户模块
 * Created by wtist on 2016/5/25.
 */
object DeAbnormalUsers {
  val usage = """
    Usage:SplitDataByType inputDir outputDir month province normalUserDir abNormalUser cc_threshold sm_threshold mm_threshold
        """
  def main(args:Array[String]): Unit = {
  //  Env.check(args, usage, false, true)
    val sc = Env.init(args, "DeAbNormalUsers")
    val inputDir = args(0)
    val outputDir = args(1)
    println("input: " + inputDir + " output: " + outputDir)

    val normalUserDir = args(4)
    val abNormalUserDir = args(5)
    println("normalUserDir: " + normalUserDir + " abNormalUserDir: " + abNormalUserDir)

    val cc_threshold = args(6).toInt
    val cc_inputDir = inputDir + File.separator + "CCDataWithCoordinateAndProvince.csv"
    val cc_outputDir = outputDir + File.separator + "CCNormalUserLoc.csv"
    val cc_normalUserDir = normalUserDir + File.separator + "CCNormalUser"
    val cc_abNormalUserDir = abNormalUserDir + File.separator + "CCAbNormalUser"
    println("cc_threshold: " + cc_threshold)
    println("cc_inputDir: " + cc_inputDir)
    println("cc_outputDir: " + cc_outputDir)
    println("cc_normalUserDir: " + cc_normalUserDir)
    println("cc_abNormalUserDir: " + cc_abNormalUserDir)
    println("start to DeAbnormalUsers cc data")
    val cc_inputRDD = sc.textFile(cc_inputDir)
    val (cc_normalUser, cc_abNormalUser) = DeAbnormalUser(cc_inputRDD, cc_threshold)                       // 获得正常用户和异常用户
    cc_normalUser.saveAsTextFile(cc_normalUserDir)
    cc_abNormalUser.saveAsTextFile(cc_abNormalUserDir)
    val cc_cleanedUserMap = cc_abNormalUser.map(x => x.split(",") match {case Array(user, province) => (user, 1)}).collectAsMap
    val cc_broadCastAbnormalUserMap = sc.broadcast(cc_cleanedUserMap)
    GetTrueUserWithProvinceAndCoordinate(cc_inputRDD, cc_broadCastAbnormalUserMap).saveAsTextFile(cc_outputDir)           //过滤异常用户记录

    val sm_threshold = args(7).toInt
    val sm_inputDir = inputDir + File.separator + "SMDataWithCoordinateAndProvince.csv"
    val sm_outputDir = outputDir + File.separator + "SMNormalUserLoc.csv"
    val sm_normalUserDir = normalUserDir + File.separator + "SMNormalUser"
    val sm_abNormalUserDir = abNormalUserDir + File.separator + "SMAbNormalUser"
    println("sm_threshold: " + sm_threshold)
    println("sm_inputDir: " + sm_inputDir)
    println("sm_outputDir: " + sm_outputDir)
    println("sm_normalUserDir: " + sm_normalUserDir)
    println("sm_abNormalUserDir: " + sm_abNormalUserDir)
    println("start to DeAbnormalUsers sm data")
    val sm_inputRDD = sc.textFile(sm_inputDir)
    val (sm_normalUser, sm_abNormalUser) = DeAbnormalUser(sm_inputRDD, sm_threshold)                       // 获得正常用户和异常用户
    sm_normalUser.saveAsTextFile(sm_normalUserDir)
    sm_abNormalUser.saveAsTextFile(sm_abNormalUserDir)
    val sm_cleanedUserMap = sm_abNormalUser.map(x => x.split(",") match {case Array(user, province) => (user, 1)}).collectAsMap
    val sm_broadCastAbnormalUserMap = sc.broadcast(sm_cleanedUserMap)
    GetTrueUserWithProvinceAndCoordinate(sm_inputRDD, sm_broadCastAbnormalUserMap).saveAsTextFile(sm_outputDir)           //过滤异常用户记录

    val mm_threshold = args(8).toInt
    val mm_inputDir = inputDir + File.separator + "MMDataWithCoordinateAndProvince.csv"
    val mm_outputDir = outputDir + File.separator + "MMNormalUserLoc.csv"
    val mm_normalUserDir = normalUserDir + File.separator + "MMNormalUser"
    val mm_abNormalUserDir = abNormalUserDir + File.separator + "MMAbNormalUser"
    println("mm_threshold: " + mm_threshold)
    println("mm_inputDir: " + mm_inputDir)
    println("mm_outputDir: " + mm_outputDir)
    println("mm_normalUserDir: " + mm_normalUserDir)
    println("mm_abNormalUserDir: " + mm_abNormalUserDir)
    println("start to DeAbnormalUsers mm data")
    val mm_inputRDD = sc.textFile(mm_inputDir)
    val (mm_normalUser, mm_abNormalUser) = DeAbnormalUser(mm_inputRDD, mm_threshold)                       // 获得正常用户和异常用户
    mm_normalUser.saveAsTextFile(mm_normalUserDir)
    mm_abNormalUser.saveAsTextFile(mm_abNormalUserDir)
    val mm_cleanedUserMap = mm_abNormalUser.map(x => x.split(",") match {case Array(user, province) => (user, 1)}).collectAsMap
    val mm_broadCastAbnormalUserMap = sc.broadcast(mm_cleanedUserMap)
    GetTrueUserWithProvinceAndCoordinate(mm_inputRDD, mm_broadCastAbnormalUserMap).saveAsTextFile(mm_outputDir)           //过滤异常用户记录

    sc.stop()
  }
}
