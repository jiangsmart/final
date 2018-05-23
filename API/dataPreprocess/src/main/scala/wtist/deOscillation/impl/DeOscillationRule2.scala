package wtist.deOscillation.impl

import wtist.Tools._


/**
 * Created by wtist on 2016/5/28.
 */
object DeOscillationRule2 {
  /**
   * 基站去震荡规则2
   * TimeDiff(sp(i).last, record(j)) <= (60 * timeThreshold)) && (GetDistance((Record(i), preRecord) ) >= disThreshold
   * @param Record : Array[(cell_id, longitude, latitude, first, last, isStablePoint)]
   * @param timeThreshold : 1 min Long
   * @param disThreshold : 10 km Long
   * @return : Array[(cell_id, longitude, latitude, first, last, isStablePoint)]
   */
  def OscillationRule2(Record: Array[(String, String, String, String, String, String)], timeThreshold: Long, disThreshold: Long): Array[(String, String, String, String, String, String)] = {
    var preRecord = ("None", "None", "None", "None", "None", "None")
    val total = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String, String)]()
    for (i <- 0 until Record.length) {
      if (preRecord._1.equals("None")) {
        if (Record(i)._6.equals("False") || Record(i)._2.equals("None")) {
          total += Record(i)
        } else {
          preRecord = Record(i)
          total += Record(i)
        }
      } else {
        if (Record(i)._6.equals("True")) {
          if (Record(i)._2.equals("None")) {
            preRecord = ("None", "None", "None", "None", "None", "None")
            total += Record(i)
          } else {
            preRecord = Record(i)
            total += Record(i)
          }
        } else {
          if (!Record(i)._2.equals("None")) {
            if (!((TimeDiff(preRecord._5, Record(i)._4) <= (60 * timeThreshold)) && (GetDistance((Record(i)._2, Record(i)._3), (preRecord._2, preRecord._3)) >= disThreshold))) {
              total += Record(i)
            }
          } else {
            total += Record(i)
          }
        }
      }
    }
    total.toArray
  }
}
