package wtist.travel.drivers

import wtist.{Arguments, Env}
import wtist.travel.impl.Migration._
/**
  * Created by chenqingqing on 2016/6/1.
  */
object Migration {
  val usage = """
    Usage: ScenicSpotAnalysis -i OtherProvStopPoint -p month -p CustomerInfo -p ScenicSpot -o MergeMigrationDataByDay -o MergeMigrationDataByMonth
              """
  def main(args:Array[String]): Unit = {
    Env.check(args, usage, true, true)
    val sc = Env.init(args, "MigrationBetweenScenicSpots")
    val inputRDD = sc.textFile(Arguments.getInputByIndex(0)).persist()
    val month = Arguments.getParamAsString(0)
    val CustomerInfo = sc.textFile(Arguments.getParamAsString(1))
    val ScenicSpot = sc.textFile(Arguments.getParamAsString(2))
    val outputPath1 = Arguments.getOutputByIndex(0)
    val outputPath2 =Arguments.getOutputByIndex(1)
    for(index <- 0 until Arguments.getInputs.length) {
      println("inputFile: "+ Arguments.getInputByIndex(index) + " outputFile: "
        + Arguments.getOutputByIndex(index))}
    val MergeMigrationDataByDay = mergeMigrationDataByDay(month,inputRDD,CustomerInfo,ScenicSpot).persist()
    MergeMigrationDataByDay.saveAsTextFile(outputPath1)
    val MergeMigrationDataByMonth = mergeMigrationDataByMonth(MergeMigrationDataByDay)
    MergeMigrationDataByMonth.saveAsTextFile(outputPath2)

    sc.stop()
  }

}
