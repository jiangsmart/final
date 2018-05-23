package wtist.dataClean.drivers

import java.io.File

import wtist.{Env, Arguments}
import wtist.dataClean.impl.SelectFields._
/**
 * 字段选择模块主函数
 * Created by wtist on 2016/5/16.
 */
object SelectFields {
  val usage = """
    Usage: SelectFileds inputDir outputDir month province
              """
  def main(args:Array[String]): Unit = {
   // Env.check(args, usage, true, true)
    val sc = Env.init(args, "SelectFields")
    val inputDir = args(0)
    val outputDir = args(1)
    println("input: " + inputDir + " output: " + outputDir)
    val month = args(2)
    val cc_inputDir = inputDir + File.separator + month+"CC.csv"
    val cc_outputDir = outputDir + File.separator + "SelectFiledCCData.csv"
    println("cc_inputDir: " + cc_inputDir)
    println("cc_outputDir: " + cc_outputDir)
    println("start to select filed cc data")
    val cc_inputRDD = sc.textFile(cc_inputDir)
    val cc_record = GetCCRecord(cc_inputRDD)
    cc_record.saveAsTextFile(cc_outputDir)

    val sm_inputDir = inputDir + File.separator + month+"SM.csv"
    val sm_outputDir = outputDir + File.separator + "SelectFiledSMData.csv"
    println("sm_inputDir: " + sm_inputDir)
    println("sm_outputDir: " + sm_outputDir)
    println("start to select filed sm data")
    val sm_inputRDD = sc.textFile(sm_inputDir)
    val sm_record = GetSMRecord(sm_inputRDD)
    sm_record.saveAsTextFile(sm_outputDir)

    val mm_inputDir = inputDir + File.separator + month+"MM.csv"
    val mm_outputDir = outputDir + File.separator + "SelectFiledMMData.csv"
    println("mm_inputDir: " + mm_inputDir)
    println("mm_outputDir: " + mm_outputDir)
    println("start to select filed mm data")
    val mm_inputRDD = sc.textFile(mm_inputDir)
    val mm_record = GetMMRecord(mm_inputRDD)
    mm_record.saveAsTextFile(mm_outputDir)
    sc.stop()
  }
}
