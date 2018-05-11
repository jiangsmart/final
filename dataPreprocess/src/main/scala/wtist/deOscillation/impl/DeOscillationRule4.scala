package wtist.deOscillation.impl

import wtist.Tools._


/**
 * 基站去震荡规则4
 * Created by wtist on 2016/5/29.
 */
object DeOscillationRule4 {
  /**
   * 基站去震荡规则4
   * @param Record  : Array[(cell_id, longitude, latitude, first, last, isStablePoint)] 记录
   * @param timeWindows 时间窗口 eg. 60秒
   * @param countThreshold 该窗口记录数
   * @param uniqCountThreshold 该窗口含有cell的个数
   * @return : Array[(cell_id, longitude, latitude, first, last, isStablePoint)]
   */
  def OscillationRule4(Record: Array[(String, String, String, String, String, String)], timeWindows: Long, countThreshold: Int, uniqCountThreshold: Int): Array[(String, String, String, String, String, String)] = {
    val windows = scala.collection.mutable.Queue[(String, String, String, String, String, String)]()
    val total = scala.collection.mutable.Stack[(String, String, String, String, String, String)]()
    var i = 0
    while (i < Record.length) {
      if (Record(i)._6.equals("False")) {
        if (windows.length == 0) {
          windows.enqueue(Record(i))
        }
        else if (TimeDiff(windows(0)._4, Record(i)._4) < timeWindows) {
          windows.enqueue(Record(i))
        } else {
          if (IsSuspicious(windows.toArray, countThreshold, uniqCountThreshold)) {
            val susSeq = new SuspiciousSequence(timeWindows, countThreshold, uniqCountThreshold)
            susSeq.addCurrent(windows.toArray)
            val (indexBefore, indexNext) = susSeq.expand(i - 1, Record)
            if (susSeq.check()) {
              // 存在环
              val (cell, lng, lat) = susSeq.remove()
              val first = Record(indexBefore)._4
              val last =
                if(Record(indexNext)._5.equals("None")) {
                  Record(indexNext)._4
                }else {
                  Record(indexNext)._5
                }
              for (j <- 0 until (i - windows.length - indexBefore) if !total.isEmpty) {
                total.pop()
              }
              windows.clear()
              i =
                if (indexNext == i - 1) {
                  windows.enqueue(Record(i))
                  i
                } else {
                  indexNext
                }
              total.push((cell, lng, lat, first, last, "False"))
            } else {
              //不存在环
              while(!windows.isEmpty &&TimeDiff(windows(0)._4, Record(i)._4) > timeWindows) {
                total.push(windows.dequeue())
              }
              windows.enqueue(Record(i))
            }

          } else {
            while(!windows.isEmpty && TimeDiff(windows(0)._4, Record(i)._4) > timeWindows) {
              total.push(windows.dequeue())
            }
            windows.enqueue(Record(i))
          }
        }
      } else if(!windows.isEmpty) {
        // 如果是stable point
        if (IsSuspicious(windows.toArray, countThreshold, uniqCountThreshold)) {
          // 如果满足suspicious 序列要求
          val susSeq = new SuspiciousSequence(timeWindows, countThreshold, uniqCountThreshold)
          susSeq.addCurrent(windows.toArray)
          val (indexBefore, indexNext) = susSeq.expand(i - 1, Record)
          if (susSeq.check()) {
            // 存在环
            val (cell, lng, lat) = susSeq.remove()
            val first = Record(indexBefore)._4
            val last =
              if(Record(indexNext)._5.equals("None")) {
                Record(indexNext)._4
              }else {
                Record(indexNext)._5
              }
            for (j <- 0 until (i - windows.length - indexBefore) if !total.isEmpty) {
              total.pop()
            }
            total.push((cell, lng, lat, first, last, "False"))
          } else {
            //不存在环
            total.pushAll(windows)
          }
        } else {
          total.pushAll(windows)
        }
        total.push(Record(i))
        windows.clear()
      }
      i += 1
    }
    if (!windows.isEmpty) {
      if (IsSuspicious(windows.toArray, countThreshold, uniqCountThreshold)) {
        // 如果满足suspicious 序列要求
        val susSeq = new SuspiciousSequence(timeWindows, countThreshold, uniqCountThreshold)
        susSeq.addCurrent(windows.toArray)
        val (indexBefore, indexNext) = susSeq.expand(i - 1, Record)
        if (susSeq.check()) {
          // 存在环
          val (cell, lng, lat) = susSeq.remove()
          val first = Record(indexBefore)._4
          val last =
            if(Record(indexNext)._5.equals("None")) {
              Record(indexNext)._4
            }else {
              Record(indexNext)._5
            }
          for (i <- 0 until (i - windows.length - indexBefore) if !total.isEmpty) {
            total.pop()
          }
          total.push((cell, lng, lat, first, last, "False"))
        } else {
          //不存在环
          total.pushAll(windows)
        }
      } else {
        total.pushAll(windows)
      }
    }
    windows.clear()
    total.toList.toArray.reverse
  }

  def IsSuspicious(windows: Array[(String, String, String, String, String, String)], countThreshold: Int, uniqCountThreshold: Int): Boolean = {
    val uniqCount = windows.map(x => x match {
      case (cell_id, longitude, latitude, first, last, isStablePoint) => cell_id
    }).distinct.length
    val counts = windows.length
    return uniqCount >= uniqCountThreshold && counts >= countThreshold
  }
}
