package wtist.deOscillation.impl

import wtist.Tools._

/**
 * Created by wtist on 2016/5/27.
 */
object StablePoint {

  /**
   *  获取静态点
   *  规则：
   *  至少出现三个及以上的重复点，且sp.last - sp.first >= timeThreshold
   * @param Record : Array[(time, cell_id, longitude, latitude)]
   * @param timeThreshold : Long 时间阈值 单位：分钟
   * @return : Array[(cell_id, longitude, latitude, first, last, isStablePoint)]
   */
  def GetStablePoint(Record: Array[(String, String, String, String)], timeThreshold : Long) : Array[(String, String, String, String, String, String)] = {
    val total = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String, String)]()
    val stable = scala.collection.mutable.ArrayBuffer[(String, String, String, String)]()
    for(i <- 0 until Record.length) {
      if(stable.length == 0) {
        stable += Record(i)
      } else if(stable(0)._2.equals(Record(i)._2)){
        stable += Record(i)
      } else {
        if(IsStablePoint(stable.toArray, timeThreshold)) {
          val first = stable(0)._1
          val last = stable(stable.length - 1)._1
          val record = stable(0) match{
            case (time, cell_id, longitude, latitude) =>
              (cell_id, longitude, latitude, first, last, "True")
          }
          total += record
        } else {
          val first = stable(0)._1
          val last =
            if(stable(stable.length - 1)._1.equals(first)) {
              "None"
            }else {
              stable(stable.length - 1)._1
            }
          val record = stable(0) match{
            case (time, cell_id, longitude, latitude) =>
              (cell_id, longitude, latitude, first, last, "False")
          }
          total += record
        }
        stable.clear()
        stable += Record(i)
      }
    }
    if(!stable.isEmpty && IsStablePoint(stable.toArray, timeThreshold)) {
      val first = stable(0)._1
      val last = stable(stable.length - 1)._1
      val record = stable(0) match{
        case (time, cell_id, longitude, latitude) =>
          (cell_id, longitude, latitude, first, last, "True")
      }
      total += record
    } else {
      val first = stable(0)._1
      val last =
        if(stable(stable.length - 1)._1.equals(first)) {
          "None"
        }else {
          stable(stable.length - 1)._1
        }
      val record = stable(0) match{
        case (time, cell_id, longitude, latitude) =>
          (cell_id, longitude, latitude, first, last, "False")
      }
    }
    stable.clear()
    total.toArray
  }


  /**
   * 判断是否是停留点
   *  规则：
   *  至少出现三个及以上的重复点，且sp.last - sp.first >= timeThreshold
   * @param Record : Array[(time, cell_id, longitude, latitude)]
   * @param timeThreshold : Long 时间阈值 单位：分钟
   * @return 如果是静态点，则返回true，否则false
   */
  def IsStablePoint(Record : Array[(String, String, String, String)], timeThreshold : Long): Boolean = {
    val Len = Record.length
    val diff = TimeDiff(Record(0)._1, Record(Len - 1)._1 )
     if(diff >= (60 * timeThreshold)) {
      return true
    }
    return false
  }
}
