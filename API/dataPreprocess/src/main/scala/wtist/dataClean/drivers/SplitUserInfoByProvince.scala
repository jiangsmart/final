package wtist.dataClean.drivers

import java.io.File

import wtist.{Env, Arguments}
import wtist.dataClean.impl.SplitUserInfoByProvince._
/**
 * 根据省份信息分割记录
 * Created by wtist on 2016/5/25.
 */
object SplitUserInfoByProvince {
  val usage = """
    Usage: SplitUserInfoByProvince inputDir outputDir month(201512) state provinceMap
              """
  def main(args:Array[String]): Unit = {
    //  Env.check(args, usage, true, true)
      val sc = Env.init(args, "SplitUserInfoByProvince")
      val inputDir = args(0)
      val outputDir = args(1)
      println("input: " + inputDir + " output: " + outputDir)
      val merged_inputDir = inputDir + File.separator + "mergeDistinctNormalUserLoc.csv"
      println("merged_inputDir: " + merged_inputDir)
      val merged_inputRDD = sc.textFile(merged_inputDir)

      val state = args(3)
      val province = args(4)
      val  provinceMap = sc.broadcast(sc.textFile(province).map(x => x.split(",") match {case Array(province, number) => (province, number)}).collectAsMap())
      val (teleHainan, tele177, teleOthers) = SplitUserByProvinceMap(merged_inputRDD, provinceMap, state)
      val month = args(2)
      println("month: " + month)
      val provinceUserLoc = outputDir + File.separator + month+"TotalLocal.csv"
      println("provinceUserLoc: " + provinceUserLoc)
      teleHainan.saveAsTextFile(provinceUserLoc)
      val UserLoc177 = outputDir + File.separator + month+"Total177.csv"
      println("UserLoc177: " + UserLoc177)
      tele177.saveAsTextFile(UserLoc177)
      val otherProvinceUserLoc = outputDir + File.separator + month+"TotalOther.csv"
      println("otherProvinceUserLoc: " + otherProvinceUserLoc)
      teleOthers.saveAsTextFile(otherProvinceUserLoc)
      sc.stop()
  }
}
