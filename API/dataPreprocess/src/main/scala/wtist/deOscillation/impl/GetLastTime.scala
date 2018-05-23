package wtist.deOscillation.impl

/**
 * Created by wtist on 2016/6/28.
 */
/**
 * 对于记录中last时间为空的记录，我们选取按时间顺序下一条记录的起始时间作为该记录的结束时间
 */
object GetLastTime {
  def GetLastTime(Record: Array[(String, String, String, String, String, String)]) : Array[(String, String, String, String, String, String)] = {
    val total = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String, String)]()
    for (i <- 0 until Record.length) {
      if(Record(i)._5.equals("None")) {
        if(i < Record.length - 1) {
          total +=( Record(i) match {case (cell_id, longitude, latitude, first, last, isStablePoint) => (cell_id, longitude, latitude, first,  Record(i + 1)._4, isStablePoint)})
        } else {
          total +=( Record(i) match {case ( cell_id, longitude, latitude, first, last, isStablePoint) => (cell_id, longitude, latitude, first, first, isStablePoint)})
        }
      } else {
        total += Record(i)
      }
    }
    total.toArray
  }
}
