package wtist.deOscillation.impl

/**
 * Created by wtist on 2016/7/21.
 */
import wtist.Tools.{GetDistance, timetostamp}

object DeSameTimeRecord {
  /**
   * 去除时间相同而基站不同的位置点
   * 对于时间相同而基站不同的记录处理方法:
       假设A|B,C,D|E是某人按时间排序的位置记录且B,C,D为时间相同而基站不同的位置记录。
       1) 计算B,C,D分别与A和E的平均距离；
       2）从B,C,D中选取平均距离最小的位置作为该时刻的位置点，过滤另外两个异常点。
   * @param Record : Array[(time, cell_id, longitude, latitude)]
   * @return : Array[(time, cell_id, longitude, latitude)]
   */
  def DeSameTimeRecord(Record: Array[(String, String, String, String)]) : Array[(String, String, String, String)] = {
    val loc = Record.toArray.sortWith((a, b) => timetostamp(a._1).toLong < timetostamp(b._1).toLong)
    val timeFreq = loc.foldLeft(Map[String, Int]())((m, c) => m + (c._1 -> (m.getOrElse(c._1, 0) + 1)))
    val total = scala.collection.mutable.ArrayBuffer[(String, String, String, String)]()
    val sameTimeSusp = scala.collection.mutable.ArrayBuffer[(String, String, String, String)]()
    var i = 0
    while (i < loc.length) {
      if(timeFreq.getOrElse(loc(i)._1, 0) > 1) {
        val preRecord =
          if(i != 0) {
            loc(i - 1)
          }else {
            ("None", "None", "None", "None")
          }
        val sameTimeCount = timeFreq.getOrElse(loc(i)._1, 0)
        val backRecord =
          if((i + sameTimeCount) < loc.length) {
            loc(i + sameTimeCount)
          } else {
            ("None", "None", "None", "None")
          }
        for(j <- 0 until sameTimeCount) {
          sameTimeSusp += loc(i + j)
        }
        val newRecord = sameTimeSusp.map{x => val averageDistance = (GetDistance((preRecord._3, preRecord._4),(x._3, x._4)) + GetDistance((backRecord._3, backRecord._4),(x._3, x._4))); (x, averageDistance)}.sortWith((a, b) =>a._2 < b._2)(0)._1
        total += newRecord
        sameTimeSusp.clear
        i = sameTimeCount + i
      } else {
        total += loc(i)
        i = i + 1
      }
    }
    val  sorted_total = total.sortWith((a, b) => timetostamp(a._1).toLong < timetostamp(b._1).toLong)
    sorted_total.toArray
  }

}
