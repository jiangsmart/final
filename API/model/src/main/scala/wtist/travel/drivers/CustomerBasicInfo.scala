package wtist.travel.drivers

import wtist.{Arguments, Env}
import wtist.travel.impl.CustomerBasicInfo._
/**
  * Created by chenqingqing on 2016/6/1.
  */
object CustomerBasicInfo {
  val usage = """
    Usage: ScenicSpotAnalysis -i otherProvOriginRec -i otherProvStopPoint -p CellTrans  -o CustomerBasicInfo
              """
  def main(args:Array[String]): Unit = {
    Env.check(args, usage, true, true)
    val sc = Env.init(args, "CustomerBasicInfo")
//    sc.hadoopConfiguration
    val OtherProvOriginRec = sc.textFile(Arguments.getInputByIndex(0))
    val OtherProvStopPoint = sc.textFile(Arguments.getInputByIndex(1))
    val CellTrans =sc.textFile(Arguments.getParamAsString(0))
    val outputPath = Arguments.getOutputByIndex(0)
    for(index <- 0 until Arguments.getInputs.length) {
      println("inputFile: "+ Arguments.getInputByIndex(index) + " outputFile: "
        + Arguments.getOutputByIndex(index))}
    val CustomerInfo = customerDetect(OtherProvOriginRec).persist()
    CustomerInfo.saveAsTextFile(outputPath+"/CustomerInfo")
    val Transportation = transportation(OtherProvStopPoint,CustomerInfo,CellTrans)
    Transportation.saveAsTextFile(outputPath+"/Transportation")
    val HomePlace = homePlaceForCustomer(OtherProvStopPoint,CustomerInfo,CellTrans)
    HomePlace.saveAsTextFile(outputPath+"/HomePlace")
    sc.stop()
  }


}
