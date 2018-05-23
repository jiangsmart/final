package wtist.activityPattern.drivers

import wtist.{Env, Arguments}
import wtist.activityPattern.impl.ActivityInfer._
/**
 * Created by Pray 
 * Date on 2016/5/25.
 */
object InferActivity {
  val usage = """
    Usage: ActivityInfer -i StablePoint -p HomeLngLat -p WorkLngLat -p CellList -p CellInHaikou -p Pois -o StopPoint -o StopPointwithActivity
              """
  def main(args:Array[String]): Unit = {
    Env.check(args, usage, false, true)
    val sc = Env.init(args, "ActivityInfo")
    val inputRDD = sc.textFile(Arguments.getInputByIndex(0)).persist()
    val HomeLngLat =sc.textFile(Arguments.getParamAsString(0))
    val WorkLngLat =sc.textFile(Arguments.getParamAsString(1))
    val CellTrans = sc.textFile(Arguments.getParamAsString(2))
    val CellInHaikou =sc.textFile(Arguments.getParamAsString(3))
    val Pois =sc.textFile(Arguments.getParamAsString(4))
    val outputStopPoint = Arguments.getOutputByIndex(0)
    val outputStopPointWithAct =Arguments.getOutputByIndex(1)
    for(index <- 0 until Arguments.getInputs.length) {
      println("inputFile: "+ Arguments.getInputByIndex(index) + " outputFile: "
        + Arguments.getOutputByIndex(index))}
    val StopPoint = GetStayPointByTime(HomeLngLat,WorkLngLat,CellInHaikou,CellTrans,inputRDD).persist()
    StopPoint.saveAsTextFile(outputStopPoint)
    val stopPointwithAct = GetActivity(Pois,StopPoint,CellTrans)
    stopPointwithAct.saveAsTextFile(outputStopPointWithAct)

    sc.stop()
  }
}
