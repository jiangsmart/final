package wtist.dataClean.drivers

import wtist.{Env, Arguments}
import wtist.dataClean.impl.SplitDataByType._
/**
 * 以数据类型分割MergedData
 * Created by Pray 
 * Date on 2016/5/25.
 */
object SplitDataByType {
  val usage = """
    Usage: SplitDataByType -i MergedDataWithMonth  -o CCDataWithMonth -o SMDataWithMonth -o MMDataWithMonth
              """
  def main(args:Array[String]): Unit = {
    Env.check(args, usage, false, true)
    val sc = Env.init(args, "SplitDataByType")
//    sc.getConf.

    val inputRDD = sc.textFile(Arguments.getInputByIndex(0)).persist()
    val outputCC = Arguments.getOutputByIndex(0)
    val outputSM =Arguments.getOutputByIndex(1)
    val outputMM =Arguments.getOutputByIndex(2)
    println("inputFile: "+ Arguments.getInputByIndex(0) + "outputFile: " + outputCC)
    val CCData=ClassifiedCCData(inputRDD)
    val MMData=ClassifiedMMData(inputRDD)
    val SMData=ClassifiedSMData(inputRDD)
    CCData.saveAsTextFile(outputCC)
    SMData.saveAsTextFile(outputSM)
    MMData.saveAsTextFile(outputMM)
    sc.stop()
  }
}
