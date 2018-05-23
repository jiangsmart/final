package wtist.deOscillation.impl

import wtist.Tools._


import scala.collection.mutable.Queue

/**
 * Created by wtist on 2016/5/28.
 */
object DeOscillationRule3 {
  /**
   * 基站去震荡规则3
   * GetSpeed(record(i), record(i+1)) * GetSpeed(record(i+1), record(i+2)) >= speedThreshold*speedThreshold &&
   * GetDistance(record(i), record(i+1)) >= distThreshold && GetDistance(record(i+1), record(i+2)) >= distThreshold &&  GetDistance(record(i), record(i+2)) <= distThreshold / 2
   * @param Record : Array[(cell_id, longitude, latitude, first, last, isStablePoint)]
   * @param speedThreshold : 250 km/h Long
   * @param distThreshold : 100 km Long
   * @return : Array[(cell_id, longitude, latitude, first, last, isStablePoint)]
   */
  def OscillationRule3(Record: Array[(String, String, String, String, String, String)], speedThreshold : Double, distThreshold : Double): Array[(String, String, String, String, String, String)] = {
    val total = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String, String)]()
    val windows = scala.collection.mutable.Queue[Int]()
    if(Record.length >= 3) {
      var index = 0
      while(windows.length < 3 && index < Record.length) {
        if(!Record(index)._2.equals("None")) {
          windows.enqueue(index)
        } else {
          while (windows.length > 0) {
            total += Record(windows.dequeue())
          }
          total += Record(index)
        }
        index += 1
      }
      for(i <- index until Record.length) {
        if(windows.length == 3) {
          if(IsRule3(windows, Record, speedThreshold, distThreshold)) {
            total += Record(windows.dequeue())
            windows.dequeue()
          } else {
            total += Record(windows.dequeue())
          }
        }
        if(!Record(i)._2.equals("None")) {
          windows.enqueue(i)
        } else {
          while (windows.length > 0) {
            total += Record(windows.dequeue())
          }
          total += Record(i)
        }
      }
      if(windows.length == 3) {
        if(IsRule3(windows, Record, speedThreshold, distThreshold)) {
          total += Record(windows.dequeue())
          windows.dequeue()
          total += Record(windows.dequeue())
        } else {
          while (windows.length > 0) {
            total += Record(windows.dequeue())
          }
        }
      } else if(windows.length > 0) {
        while (windows.length > 0) {
          total += Record(windows.dequeue())
        }
      }
    } else {
      total ++= Record
    }
    total.toArray
  }

  /**
   * 判断是否满足规则3
   * @param windows
   * @param Record
   * @param speedThreshold
   * @param distThreshold
   * @return
   */
  def IsRule3(windows: Queue[Int], Record: Array[(String, String, String, String, String, String)], speedThreshold: Double, distThreshold: Double): Boolean = {
    val dist12 = GetDistance((Record(windows(0))._2,Record(windows(0))._3), (Record(windows(1))._2,Record(windows(1))._3))
    val dist13 = GetDistance((Record(windows(0))._2,Record(windows(0))._3), (Record(windows(2))._2,Record(windows(2))._3))
    val dist23 = GetDistance((Record(windows(1))._2,Record(windows(1))._3), (Record(windows(2))._2,Record(windows(2))._3))
    val speed12 = GetSpeed(dist12, Record(windows(0))._4, Record(windows(1))._4)
    val speed23 = GetSpeed(dist23, Record(windows(1))._4, Record(windows(2))._4)
    val flag = if(Record(windows(1))._6.equals("False")&&(speed12*speed23 > (speedThreshold*speedThreshold)) && dist12 >= distThreshold && dist23 >= distThreshold && dist13 <= (distThreshold / 2)) {
      true
    } else {
      false
    }
    flag
  }
}
