package wtist.dataClean.drivers

import java.io.File

import wtist.{Env, Arguments}
import wtist.dataClean.impl.JoinUserCoordinateAndProvinceInfo._
/**
 * 连接用户经纬度和省份信息
 * Created by wtist on 2016/5/25.
 */
object JoinUserCoordinateAndProvince {
  val usage = """
    Usage: JoinUserLatLontAndProvince inputDir outputDir month province CellListDir TeleUserInfoDir
              """
  def main(args:Array[String]): Unit = {
    //Env.check(args, usage, true, true)
    val sc = Env.init(args, "JoinUserWithCoordinateAndProvince")
    val inputDir = args(0)
    val outputDir = args(1)
    println("input: " + inputDir + " output: " + outputDir)
    val CellListDir = args(4)
    println("CellListDir: " + CellListDir + " DataType: cell,longitude,latitude")
    val CellList = sc.textFile(CellListDir)
    val cellInfo = CellList.map { x =>
          x.split(",") match {
        case Array(cell,longitude,latitude) => (cell,(longitude,latitude))
      }
    }
    val broadCastCellMap = sc.broadcast(cellInfo.collectAsMap)

    val TeleUserInfoDir = args(5)
    println("TeleUserInfoDir: " + TeleUserInfoDir + " DataType: hashmd5,md5,operator,province")
    val userInfo = sc.textFile(TeleUserInfoDir)
    val teleUserInfo =
      if (userInfo.first().length != 2) {
        val result = userInfo.map(x =>
          x.split(",") match {
            case Array(hashmd5, md5, operator, province) => (hashmd5, (operator, province))
          }).
          filter(x => x match {case (hashmd5, (operator, province)) => operator.equals("1")}).
          map(x => x match {
            case (user, (operator, province)) => (user, province)
          })
        result
      } else {
        userInfo.map(x => x.split(",") match {case Array(user, province) => (user, province)})
      }
    val teleUserInfoMap =  sc.broadcast(teleUserInfo.collectAsMap)

    val cc_inputDir = inputDir + File.separator + "SelectFiledCCData.csv"
    val cc_outputDir = outputDir + File.separator + "CCDataWithCoordinateAndProvince.csv"
    println("cc_inputDir: " + cc_inputDir)
    println("cc_outputDir: " + cc_outputDir)
    println("start to join user coordinate and province cc data")
    val cc_inputRDD = sc.textFile(cc_inputDir)
    GetUserCoordinateAndProvince(cc_inputRDD, teleUserInfoMap, broadCastCellMap).saveAsTextFile(cc_outputDir)

    val sm_inputDir = inputDir + File.separator + "SelectFiledSMData.csv"
    val sm_outputDir = outputDir + File.separator + "SMDataWithCoordinateAndProvince.csv"
    println("sm_inputDir: " + sm_inputDir)
    println("sm_outputDir: " + sm_outputDir)
    println("start to join user coordinate and province sm data")
    val sm_inputRDD = sc.textFile(sm_inputDir)
    GetUserCoordinateAndProvince(sm_inputRDD, teleUserInfoMap, broadCastCellMap).saveAsTextFile(sm_outputDir)

    val mm_inputDir = inputDir + File.separator + "SelectFiledMMData.csv"
    val mm_outputDir = outputDir + File.separator + "MMDataWithCoordinateAndProvince.csv"
    println("mm_inputDir: " + mm_inputDir)
    println("mm_outputDir: " + mm_outputDir)
    println("start to join user coordinate and province mm data")
    val mm_inputRDD = sc.textFile(mm_inputDir)
    GetUserCoordinateAndProvince(mm_inputRDD, teleUserInfoMap, broadCastCellMap).saveAsTextFile(mm_outputDir)

    sc.stop()
  }
}
