package wtist.deOscillation.impl

import wtist.Tools
import Tools._


/**
 * Created by wtist on 2016/5/27.
 */
object DeOscillationRule1 {

  /**
   * 基站去震荡规则1
   * TimeDiff(sp(i + 1).first, sp(i).last) <= timeThreshold 则去除sp(i)-sp(i+1)中间的点
   * @param Record : Array[(cell_id, longitude, latitude, first, last, isStablePoint)]
   * @param timeThreshold : 时间阈值 单位:分钟
   * @return Array[(cell_id, longitude, latitude, first, last, isStablePoint)]
   */
  def OscillationRule1(Record: Array[(String, String, String, String, String,String)], timeThreshold: Long): Array[(String, String, String, String, String, String)] = {
    var preRecord = ("None", "None", "None", "None", "None", "None")
    val tmp = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String, String)]()
    val total = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String, String)]()
    for (i <- 0 until Record.length) {
      if (preRecord._1.equals("None")) {
        if (Record(i)._6.equals("True")) {
          preRecord = Record(i)
          total += Record(i)
        } else {
          total += Record(i)
        }
      } else {
        if (Record(i)._6.equals("True")) {
          if (preRecord._1.equals(Record(i)._1) && TimeDiff(preRecord._5, Record(i)._4) <= (60 * timeThreshold)) {
            total += Record(i)
          } else {
            total ++= tmp
            total += Record(i)
          }
          tmp.clear()
          preRecord = Record(i)
        } else {
            tmp += Record(i)
          }
        }
    }
    if (tmp.length != 0) {
      total ++= tmp
      tmp.clear()
    }
    total.toArray
  }

}
