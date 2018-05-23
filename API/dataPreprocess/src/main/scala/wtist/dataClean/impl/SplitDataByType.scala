package wtist.dataClean.impl

import org.apache.spark.rdd.RDD

/**
 * Created by Pray 
 * Date on 2016/5/25.
 */
object SplitDataByType {
  /**
   * 挑选语音数据，保留字段
   * @param RawData
   * @return RDD[String] = [cdr_type+","+time+","+call_type+","+calling+","+called+","+talktime+","+start_cell+","+end_cell]
   */
  def ClassifiedCCData(RawData:RDD[String]):RDD[String] ={
    val cc_data = RawData.map(_.split(",",-1)).filter{x => val cdr_type = x(0); cdr_type.equals("1")}.map{x =>
      x(0)+","+x(1)+","+x(6)+","+x(7)+","+x(8)+","+x(10)+","+x(15)+","+x(17)}
    cc_data
  }
  /**
   * 挑选短信数据，保留字段
   * @param RawData
   * @return RDD[String] = [cdr_type+","+time+","+sms_type+","+calling+","+called+","+start_cell]
   */
  def ClassifiedSMData(RawData:RDD[String]):RDD[String] ={
    val sm_data = RawData.map(_.split(",",-1)).filter{x => val cdr_type = x(0); cdr_type.equals("2")}.map{x =>
      x(0)+","+x(1)+","+x(6)+","+x(8)+","+x(9)+","+x(14)}
    sm_data
  }

  /**
   * 挑选位置数据，保留字段
   * @param RawData
   * @return RDD[String] = [cdr_type+","+time+","+mm_type+","+mdn+","+start_cell]
   */
  def ClassifiedMMData(RawData:RDD[String]):RDD[String] ={
    val mm_data = RawData.map(_.split(",",-1)).filter{x => val cdr_type = x(0); cdr_type.equals("3")}.map{x =>
      x(0)+","+x(1)+","+x(6)+","+x(8)+","+x(12)}
    mm_data
  }

}
