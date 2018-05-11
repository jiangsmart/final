package wtist.dataClean.impl

import org.apache.spark.rdd.RDD
import wtist.Tools._

/**
 * sm,cc,mm字段筛选阶段
 * Created by wtist on 2016/5/16.
 */

object SelectFields {
  /**
   * CC文件提取字段函数
   * @param CC : RDD[cdr_type+","+time+","+call_type+","+calling+","+called+","+talk_time+","+start_cell+","+end_cell]
   * @param format: yyyyMMddHHmmss
   * @return record: RDD[user+","+time+","+cell+","+cdr_type]
   */
  def GetCCRecord(CC: RDD[String], format: String = "yyyyMMddHHmmss"): RDD[String] = {
    val record = CC.map(_.split(",")).filter(x => x.length == 8)
          .filter{x  => val call_type = x(2);call_type.equals("0") || call_type.equals("1")}
           .map{x =>
              val cdr_type = x(0)
              val time = x(1)
              val call_type = x(2)
              val calling = x(3)
              val called = x(4)
              val talk_time = x(5)
              val start_cell = x(6)
              val end_cell = x(7)
              val end_time = stamptotime(timetostamp(time, format).toLong + talk_time.toLong, format);
              val user =
                if(call_type.equals("0"))
                  calling
                else
                  called
              val  result = Array((time, start_cell, cdr_type), (end_time, end_cell, cdr_type));
            (user, result)
        }.flatMapValues(x => x)
          .filter(x =>
            x match {case (user, (time, cell, cdr_type)) =>  !cell.equals("") && !cell.equals("0") && user.length == 32})
             .map(x => x match {case (user, (time, cell, cdr_type)) => user+","+time+","+cell+","+cdr_type})
    record
  }

  /**
   *  SM文件提取字段函数
   * @param SM：RDD[cdr_type+","+time+","+sms_type+","+calling+","+called+","+start_cell]
   * @return record: RDD[user+","+time+","+cell+","+cdr_type]
   */
  def GetSMRecord(SM: RDD[String]): RDD[String] = {
    val record = SM.map(_.split(",")).filter(x => x.length == 6)
          .filter{x => val sms_type = x(2); val start_cell = x(5); (sms_type.equals("0") || sms_type.equals("1")) && !start_cell.equals("0")}
          .map{x =>
              val cdr_type = x(0)
              val time = x(1)
              val sms_type = x(2)
              val calling = x(3)
              val called = x(4)
              val start_cell = x(5)
              val user =
              if(sms_type.equals("0"))
                  calling
              else
                  called
            (user, time, start_cell, cdr_type)
        }.filter(x =>
            x match {case (user, time, start_cell, cdr_type) =>
                                            !start_cell.equals("") && !start_cell.equals("0") && user.length == 32})
          .map(x => x match {case (user, time, cell, cdr_type) => user+","+time+","+cell+","+cdr_type})
    record
  }

  /**
   * MM文件提取字段函数
   * @param MM: RDD[cdr_type+","+time+","+mm_type+","+mdn+","+start_cell]
   * @return record: RDD[user+","+time+","+cell+","+cdr_type]
   */
  def GetMMRecord(MM: RDD[String]): RDD[String] = {
    val record = MM.map(_.split(",")).filter(x => x.length == 5)
          .filter(x => x match {case Array(cdr_type, time, mm_type, mdn, start_cell) =>
                      !start_cell.equals("0") && !start_cell.equals("")})
            .map(x => x match  {case Array(cdr_type, time, mm_type, mdn, start_cell) =>
                      mdn+","+time+","+start_cell+","+cdr_type})
    record
  }
}
