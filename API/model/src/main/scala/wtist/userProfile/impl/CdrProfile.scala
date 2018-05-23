package wtist.userProfile.impl

import org.apache.spark.rdd.RDD

/**
  * Created by chenqingqing on 2016/5/25.
  */
object CdrProfile {
  /**
    * 得到用户的cdr特征
    *
    * @param CDRData :RDD[String] 语音通话详单
    * @param userHome:RDD[String] 用户的家
    * @param userWork:RDD[String] 用户的工作地点
    * @return  RDD[String] = RDD[user+","+feature]
    */
  def cdrFeat(CDRData:RDD[String],userHome:RDD[String],userWork:RDD[String]):RDD[String] ={
    val Rec = CDRData.flatMap{x=> val slice = x.split(",")
      Seq(((slice(3),slice(4)),(1,slice(5).toInt)),((slice(4),slice(3)),(1,slice(5).toInt)))
    }.filter{x=> x._1._1.equals("") == false && x._1._2.equals("") == false}.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
    val recInfo = Rec.map{x=> (x._1._1,x._2)}
    val recUsers = Rec.map{x=> x._1}
    val contactorNum = recUsers.groupByKey().map{x=> val arr = x._2.toArray;(x._1,arr.length)}
    val contactorFreDur = recInfo.reduceByKey((a,b) => (a._1 + b._1,a._2+b._2))
    val userContactInfo = contactorNum.join(contactorFreDur)
      .map{x=> (x._1,x._2._1 +"," + x._2._2._1 +","+ x._2._2._2)}//得到每个用户联系人个数，总通话次数，总通话时长
    val home = userHome.map{x=>
        val line = x.split(",")
        (line(0),line(1))}
        .distinct()//得到用户的家,(user,home)
    val family = recUsers.join(home)
        .map{x=> (x._2._1,(x._1,x._2._2))}
        .join(home)
        .map{x=> (x._1,x._2._1._1,x._2._2,x._2._1._2)}
        .filter{x=> x._3.equals(x._4) == true}
        .map{x=> (x._1,x._2)}
        .distinct()//family就是用户的家人圈
    val familyNum = family.map{x=> (x._1,1)}.reduceByKey(_+_) //计算用户家人的个数
    val work = userWork.map{x=>
        val line = x.split(",")
        (line(0),line(1))}
        .distinct()//得到用户的家,(user,workplace)
    val colleague = recUsers.join(work)
        .map{case(user1,(user2,workplace1)) =>
          (user2,(user1,workplace1))}
        .join(work)
        .map{case(user2,((user1,workplace1),workplace2)) =>
          (user1,user2,workplace1,workplace2)}
        .filter{x=> x._3.equals(x._4) == true}
        .map{x=> (x._1,x._2)}
        .distinct()//colleague就是用户的同事圈
    val colleagueNum = colleague.map{x=> (x._1,1)}.reduceByKey(_+_) //计算用户同事的个数

    val friend = recUsers.subtract(family).subtract(colleague)
    val friendNum = friend.map{x=> (x._1,1)}.reduceByKey(_+_)

    val familyRec = family.map{x=> (x,1)}
      .join(Rec)
      .map{x=> (x._1._1,x._2._2)}
      .reduceByKey((a,b) => (a._1+b._1,a._2 +b._2))
      .map{x=> (x._1,x._2._1+","+x._2._2)}

    val colleagueRec = colleague.map{x=> (x,1)}
      .join(Rec)
      .map{x=> (x._1._1,x._2._2)}
      .reduceByKey((a,b) => (a._1+b._1,a._2 +b._2))
      .map{x=> (x._1,x._2._1+","+x._2._2)}

    val friendRec = friend.map{x=> (x,1)}
      .join(Rec)
      .map{x=> (x._1._1,x._2._2)}
      .reduceByKey((a,b) => (a._1+b._1,a._2 +b._2))
      .map{x=> (x._1,x._2._1+","+x._2._2)}

    val r0 = userContactInfo.leftOuterJoin(familyNum)
      .map{x=>
        val familyNum = x._2._2.getOrElse(0)
        val user = x._1
        val basic = x._2._1
        (user,basic+","+familyNum)
      }
    val r1 = r0.leftOuterJoin(familyRec)
      .map{x=>
        val familyRec = x._2._2.getOrElse("0,0")
        val user = x._1
        val forwardFeat = x._2._1
        (user,forwardFeat+","+familyRec)
      }
    val r2 = r1.leftOuterJoin(colleagueNum)
      .map{x=>
        val colleagueNum = x._2._2.getOrElse(0)
        val user = x._1
        val forwardFeat = x._2._1
        (user,forwardFeat+","+colleagueNum)
      }
    val r3 = r2.leftOuterJoin(colleagueRec)
      .map{x=>
        val colleagueRec = x._2._2.getOrElse("0,0")
        val user = x._1
        val forwardFeat = x._2._1
        (user,forwardFeat+","+colleagueRec)
      }
    val r4 = r3.leftOuterJoin(friendNum)
      .map{x=>
        val friendNum = x._2._2.getOrElse(0)
        val user = x._1
        val forwardFeat = x._2._1
        (user,forwardFeat+","+friendNum)
      }
    val r5 = r4.leftOuterJoin(friendRec)
      .map{x=>
        val friendRec = x._2._2.getOrElse("0,0")
        val user = x._1
        val forwardFeat = x._2._1
        (user,forwardFeat+","+friendRec)
      }

    val output= r5.map{x=> x._1 +","+x._2}
    output

  }

}
