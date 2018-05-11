package wtist.travel.drivers

import wtist.{Arguments, Env}
import wtist.travel.impl.ScenicSpotAnalysis._
/**
  * Created by chenqingqing on 2016/6/1.
  */
object ScenicSpotAnalysis {
  val usage = """
    Usage: ScenicSpotAnalysis -i otherProvOriginRec -i otherProvStopPoint -p month  -p CustomerBasicInfoPath -p ScenicSpot -o ScenicSpotAnalysis
              """
  def main(args:Array[String]): Unit = {
    Env.check(args, usage, true, true)
    val sc = Env.init(args, "ScenicSpotAnalysis")
    val OtherProvOriginRec = sc.textFile(Arguments.getInputByIndex(0))
    val OtherProvStopPoint = sc.textFile(Arguments.getInputByIndex(1))
    val month = Arguments.getParamAsString(0)
    val outputPath = Arguments.getOutputByIndex(0)
    val CustomerInfo = sc.textFile(Arguments.getParamAsString(1)+"/CustomerInfo")
    val Transportation = sc.textFile(Arguments.getParamAsString(1)+"/Transportation")
    val ScenicSpot = sc.textFile(Arguments.getParamAsString(2))
    for(index <- 0 until Arguments.getInputs.length) {
      println("inputFile: "+ Arguments.getInputByIndex(index) + " outputFile: "
        + Arguments.getOutputByIndex(index))}
    val customerFlowByProv = customerFlowCount(sc,OtherProvOriginRec,CustomerInfo,month)
    customerFlowByProv.saveAsTextFile(outputPath+"/customerFlowByProv")
    val scenicCustomerByProvByMonth = scenicCustomerFlowByMonth(OtherProvOriginRec,CustomerInfo,ScenicSpot)
    scenicCustomerByProvByMonth.saveAsTextFile(outputPath+"/scenicCustomerByProvByMonth")
    val scenicCustomerByProvByDay = scenicCustomerFlowByProvByDay(OtherProvOriginRec,CustomerInfo,ScenicSpot)
    scenicCustomerByProvByDay.saveAsTextFile(outputPath+"/scenicCustomerFlowByProvByDay")
    val scenicCustomerByMonth = scenicCustomerFlowByMonth(OtherProvOriginRec,CustomerInfo,ScenicSpot)
    scenicCustomerByMonth.saveAsTextFile(outputPath+"/scenicCustomerFlowByMonth")
    val scenicCustomerByDay = scenicCustomerFlowByDay(OtherProvOriginRec,CustomerInfo,ScenicSpot)
    scenicCustomerByDay.saveAsTextFile(outputPath+"/scenicCustomerFlowByDay")
    val provinceByTrans = provinceByTransMode(Transportation,CustomerInfo)
    provinceByTrans.saveAsTextFile(outputPath+"/provinceByTransMode")
    val scenicAveStayDur = scenicSpotAveStayDur(OtherProvOriginRec,CustomerInfo,ScenicSpot)
    scenicAveStayDur.saveAsTextFile(outputPath+"/scenicSpotAveStayDur")
    val scenicStayDurCount = scenicSpotStayDurCount(OtherProvOriginRec,CustomerInfo,ScenicSpot)
    scenicStayDurCount.saveAsTextFile(outputPath+"/scenicSpotStayDurCount")
    val stayDaysCount = customerStayDaysCount(sc,CustomerInfo)
    stayDaysCount.saveAsTextFile(outputPath+"/customerStayDaysCount")
    val InAndOutCustomerCount = everyDayInAndOutCustomerCount(sc,CustomerInfo)
    InAndOutCustomerCount.saveAsTextFile(outputPath+"/everyDayInAndOutCustomerCount")



    sc.stop()
  }

}
