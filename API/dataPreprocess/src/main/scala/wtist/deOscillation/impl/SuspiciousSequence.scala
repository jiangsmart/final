package wtist.deOscillation.impl

import wtist.Tools._


/**
 * Created by wtist on 2016/5/13.
 */
class SuspiciousSequence  (val timeWindows: Long, val countThreshold: Int, val uniqCountThreshold: Int) {
  val SuspSequence = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String, String)]()
  val expandCurrent = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String, String)]()
  val expandBefore = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String, String)]()
  val expandNext = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String, String)]()
  val CellSet = scala.collection.mutable.HashSet[String]()

  def addCurrent(records: Array[(String, String, String, String, String, String)]): Unit = {
    expandCurrent ++= records
    CellSet ++= (records.map(x => x match {
      case (cell_id, longitude, latitude, first, last, isStablePoint) => cell_id
    }).distinct)
  }

  /**
   * 清除before,current,next buffer
   */
  def clearBuffer() = {
    expandBefore.clear()
    expandCurrent.clear()
    expandNext.clear()
  }

  /**
   * check 阶段
   */
  def check(): Boolean = {
    val indexMap = scala.collection.mutable.HashMap[String, Int]()
    var flag = false
    for (i <- 0 until SuspSequence.length) {
      if (i - indexMap.getOrElseUpdate(SuspSequence(i)._1, i)  > 1) {
        flag = true
        return flag
      }
    }
    flag
  }

  /**
   * remove 阶段 返回中心基站编号及经纬度
   * @return （cell, lng, lat）
   */
  def remove(): (String, String, String) = {
    Score()
  }

  /**
   * expand 阶段 主要是向前向后拓展suspicious序列 并返回suspicious序列的起始index和结束index
   * @param index 当前处理记录编号
   * @param records 某人的记录集合
   * @return （indexBefore, indexNext）
   */
  def expand(index: Int, records: Array[(String, String, String, String, String, String)]): (Int, Int) = {
    //    println("index: "+index)
    val startIndex = index - expandCurrent.length + 1 //该窗口起始下标
    val indexBefore = ExpandBefore(startIndex, records)
    val indexNext = ExpandNext(index, records)
    SuspSequence ++= expandBefore
    SuspSequence ++= expandCurrent
    SuspSequence ++= expandNext
    clearBuffer()
    (indexBefore, indexNext)
  }

  /**
   * 向前扩展 look-back阶段 suspicious序列的起始index
   * @param index 当前索引值
   * @param records 记录
   * @return
   */
  def ExpandBefore(index: Int, records: Array[(String, String, String, String, String, String)]): Int = {
    var i = index - 1
    val tmpBefore = scala.collection.mutable.ArrayBuffer[(String, String, String, String, String, String)]()
    while (i >= 0 && CellSet.contains(records(i)._1) && TimeDiff(records(i)._4, records(index)._4) <= timeWindows && records(i)._6.equals("False")) {
      tmpBefore += records(i)
      i -= 1
    }
    expandBefore ++= tmpBefore.reverse
    val indexBefore =
      if (expandBefore.isEmpty) {
        index
      } else {
        index - expandBefore.length
      }
    indexBefore
  }

  /**
   * 向后扩展 look-after阶段 suspicious序列的结束index
   * @param index 当前索引值
   * @param records 记录
   * @return
   */
  def ExpandNext(index: Int, records: Array[(String, String, String, String, String, String)]): Int = {
    var i = index + 1
    while (i < records.length && CellSet.contains(records(i)._1) && TimeDiff(records(i)._4, records(index)._4) <= timeWindows && records(i)._6.equals("False")) {
      expandNext += records(i)
      i += 1
    }
    val indexNext =
      if (expandNext.isEmpty) {
        index
      } else {
        index + expandNext.length
      }
    indexNext
  }

  /**
   * 计算suspicious序列的中心点,并返回该基站的编号及经纬度
   * @return (cell, lng, lat)
   */
  def Score(): (String, String, String) = {
    val Freq = SuspSequence.foldLeft(Map[String, Int]())((m, c) => m + (c._1 -> (m.getOrElse(c._1, 0) + 1))) // 统计每个基站出现的频次
    var max = ("", "", "", 0.0)
    for (i <- 0 until SuspSequence.length) {
      val allDistance = SuspSequence.map{x => val distance = GetDistance((SuspSequence(i)._2, SuspSequence(i)._3), (x._2, x._3)); distance}.filter(x => x != 100000.0 && x != 0.0)
      val averageDistance =
        if (allDistance.length > 0) {
          allDistance.reduce(_ + _) / allDistance.length
        } else {
          100000.0
        }
      val score = Freq.getOrElse(SuspSequence(i)._1, 0) / averageDistance
      if (score > max._4) {
        max = (SuspSequence(i)._1, SuspSequence(i)._2, SuspSequence(i)._3, score)
      }
    }
    (max._1, max._2, max._3)
  }
}

