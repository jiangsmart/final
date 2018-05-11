package wtist.activityPattern.impl

import org.apache.spark.rdd.RDD
import wtist.Tools


import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Created by Pray 
 * Date on 2016/5/25.
 */
object ActivityInfer {
  /**
   * 返回海口地区的所有用户的家和停留点(停留点时长1小时到24小时之间，且家和工作地点不同)
   * @param homeLngLat RDD[String]=RDD[user,ci,lgn.lat]
   * @param Cell   海口基站列表（region,ifurban,ifouter,ci,bscid,jizhanid,cellid,cellname,angle,lgn,lat,radius,translgn,translat）
   * @param CellTrans 全部基站（region,ifurban,ifouter,ci,bscid,jizhanid,cellid,cellname,angle,lgn,lat,radius,translgn,translat）
   * @param filteredLoc RDD[String]=RDD[day,user,time,ci,lgn,lat,duration]
   * @return  RDD[String] = RDD[user,work,home,stoptime,stopci,duration,translgn,translat,radius,ifurban,ifouter]
   */

  def GetStayPointByTime(homeLngLat:RDD[String],workLngLat:RDD[String],Cell:RDD[String],CellTrans:RDD[String],filteredLoc:RDD[String]): RDD[String] ={
    val workandhome = homeLngLat.map(_.split(",")).map{x=> x match{
         case Array(user,ci,lgn,lat) =>(user,ci)//得到所有用户的家
         }}.leftOuterJoin(workLngLat.map(_.split(","))
         .map{x=> x match { case Array(user,ci,lng,lat)=> (user,ci) //得到用户的工作地点
         }}).map{x => val user=x._1;
         val homecell =x._2._1;
         val workcell =x._2._2.getOrElse("None");
         (homecell,(user,workcell))}//合并
         .filter(x=> !(x._1.equals("None")))//过滤掉家为none的用户
         .filter(x=> !(x._2._2.equals("None")))//过滤掉工作地点为none的用户
         .filter(x=> !(x._1.equals(x._2._2)));//得到工作地点和家不一样的用户

    val userindistrict = Cell.map(_.split(","))
        .map(x=>  x match{ case Array(region,ifurban,ifouter,ci,bscid,
        jizhanid,cellid,cellname,angle,lgn,lat,radius,translgn,translat)
         => (ci,translgn+","+translat+","+radius)//将海口基站以(cellid,coordinateinfo)的形式表示出来
         }).leftOuterJoin(workandhome)//用海口基站筛选出家在海口的用户
        .map{x => val ci =x._1; val userandwork = x._2._2.getOrElse(("None","None"));
        (userandwork._1,userandwork._2+","+ci)}
      .filter{x=> (!x._1.equals("None"))}//过滤掉没得到用户id的数据

    val userwithstop = userindistrict.leftOuterJoin(
        filteredLoc.map(_.split(","))
        .map(x=> x match{case Array(day,user,time,ci,duration,lgn,lat)=>
         (user,time+","+ci+","+duration)}))//保留stablepoint里面的用户，时间，停留点和时长字段
        .map{x=> val homeandworkcell =x._2._1;
         val user =x._1;
         val stop =x._2._2.getOrElse("None,None,None");
        user+","+homeandworkcell+","+stop}//最终保留用户，家，工作地点，停留点，时间，时长

    val staypoint =  userwithstop.map(_.split(",")).map{x =>x match{
        case Array(user,workcell,homecell,stoptime,stopci,duration) =>
        (stopci,(user,homecell,workcell,stoptime,duration))}}
        .filter(x=> (x._2._5.toDouble>1.0)&&(x._2._5.toDouble<24.0))//过滤出来停留时长在1小时以上和24小时以下的停留点
        .leftOuterJoin(CellTrans.map(_.split(","))//给每个停留点标注该点的经纬度信息
        .filter(_.length==14)//过滤掉基站信息缺失的基站
        .map(x=>  x match{case Array(region,ifurban,ifouter,ci,bscid,
        jizhanid,cellid,cellname,angle,lgn,lat,radius,translgn,translat)
         => (ci,translgn+","+translat+","+radius+","+ifurban+","+ifouter)}))//保留基站信息
        .map{x=> val stopci =x._1; val coorinfo = x._2._2.getOrElse("None,None,None,None,None");
        val stoppoint =x._2._1; (stopci+","+coorinfo,stoppoint)}
        .map{x=>val stopciinfo = x._1; val user = x._2._1;val homecell =x._2._2;
        val workcell =x._2._3; val stoptime =x._2._4;val duration =x._2._5;
        (user+","+homecell+","+workcell+","+stoptime+","+duration+","+stopciinfo)}
        .map(_.split(",")).map{x=> x match{
        case Array(user,homecell,workcell,stoptime,duration,
        stopci,translgn,translat,radius,ifurban,ifouter)=>
        (user+","+homecell+","+workcell,(stoptime,duration,
        stopci,translgn,translat,radius,ifurban,ifouter))
         }}.groupByKey()//将相同用户的所有停留点group
        .map{x=> val user=x._1;
        val sorted_arr=x._2.toArray.sortWith((a,b)=>(a._1<b._1)); //按时间排序
        (user,sorted_arr) }.flatMapValues(x=>x)
      .map{x=> x match{
        case (user,(stoptime,duration,stopci,
        translgn,translat,radius,ifurban,ifouter))=>
          (user+","+stoptime+","+stopci+","+duration+","
            +translgn+","+translat+","+radius+","+ifurban+","+ifouter)}}
    staypoint
  }
  /**
   * 得到某个停留点的所有pois信息；
   * @param coorinfo （lgn,lat,radius,ifurban,ifouter,angle）
   * @param Pois(name,address,lat,lgn,uid,distance,tpe,tag,price,shophours,overrating,taste,service,environment,facility,hygiene,technology,comment)
   * @return (美食，酒店数，购物中心数，景点数，运动健身，休闲，医疗，交通)
   */
  def GetRegionPois(coorinfo:String,Pois: Array[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]):(Int,Int,Int,Int,Int,Int,Int,Int)={
    val fivelat: Double = 0.004495//每500米纬度变化
    val fivelgn: Double = 0.004790//每500米经度变化
    val coinfo :Array[String] = coorinfo.split(",")
    val lgn:Double = coinfo(0).toDouble;
    val lat:Double= coinfo(1).toDouble;
    val radius =coinfo(2).toDouble;
    //  val ifurban :String =coinfo(3);
    // val ifouter:String = coinfo(4);
    // val angle=coinfo(5);
    val num :Int = Math.ceil(radius.toDouble/500.0).toInt
    val regionTop:Double =lat+ (num*fivelat);
    val regionLow:Double =lat-(num*fivelat);
    val regionLeft:Double =lgn-(num*fivelgn);
    val regionRight:Double =lgn+(num*fivelgn);//确定所在停留点区域范围
    val filteredpois = Pois.filter{x=> val lat =x._3.toDouble; val lgn =x._4.toDouble;
        (lat<regionTop)&&(lat>regionLow)&&(lgn>regionLeft)&&(lgn<regionRight)}//筛选在区域内的POI
        .map{x=> x match{
      case (name,address,lat_1,lgn_1,uid,distance,tpe,tag,price,
      shophours,overrating,taste,service,environment,facility,hygiene,technology,comment)
      => val distance= Tools.GetDistance(lgn,lat,lgn_1.toDouble,lat_1.toDouble);
      (name,lat_1,lgn_1,tpe,tag,price,distance.toString())}}
        .filter(_._7.toDouble<radius)//在区域中筛选出在半径之内的POI

    val category = new HashMap[String,Int]()
    val filtered_category=mutable.HashMap("美食"-> 0,"医疗"->0,"交通设施"->0,
      "旅游景点"->0,"购物"->0,"运动健身"->0,"酒店"->0,"休闲娱乐"->0)
    for(i<- 0 until filteredpois.length){
      if(category.contains(filteredpois(i)._5)){
        val num= category.getOrElse(filteredpois(i)._5,0)
        category.update(filteredpois(i)._5,num+1)
      }else{
        category.put(filteredpois(i)._5,1)
      }
    }//得到每种类型的POI的数量
    val new_category = category-("医疗;药店")-("购物;便利店");//去掉与活动无关联的POI
    for((k,v)<- new_category){
      for((key,value)<-filtered_category){
        if(k.contains(key)){
          val tmp = filtered_category.getOrElse(key,0)
          filtered_category.update(key,tmp+v)
        }
      }
    }
    val dinning =filtered_category.getOrElse("美食",0);
    val hotel = filtered_category.getOrElse("酒店",0);
    val shopping=filtered_category.getOrElse("购物",0);
    val spot = filtered_category.getOrElse("旅游景点",0);
    val sport =filtered_category.getOrElse("运动健身",0);
    val recreat =filtered_category.getOrElse("休闲娱乐",0)
    val medical =filtered_category.getOrElse("医疗",0);
    val transportation =filtered_category.getOrElse("交通设施",0);
    (dinning,hotel,shopping,spot,sport,recreat,medical,transportation)
  }
  /**
   * 得到某个用户的POI打分信息
   * @param Pois(name,address,lat,lgn,uid,distance,tpe,tag,price,shophours,overrating,taste,service,environment,facility,hygiene,technology,comment)
   * @param cellinfo Map(ci,translng+","+translat+","+radius)
   * @param user (stoppoint,stoppoints)
   * @return  StayPoint(各项Poi评分)
   */
  def GetUserPoisScores(Pois: Array[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)],cellinfo:Map[String, String],user:(String,Array[String])):Staypoint={

    val stayp=user._1; val stay_arr =user._2;
    val sumpois= new ArrayBuffer[(String,(Int,Int,Int,Int,Int,Int,Int,Int))]();
    for(i<- 0 until stay_arr.length){//去掉异常基站
    val coorinfo =cellinfo.getOrElse(stay_arr(i),"None")
      if(!coorinfo.equals("None")){
        val pois =GetRegionPois(coorinfo,Pois)
        sumpois += ((stay_arr(i),pois))
      }
    }
    val coor =cellinfo.getOrElse(stayp,"None")
    if(!coor.equals("None")){//判断是否为基站列表中不包含的基站点
    val elem=GetRegionPois(coor,Pois)
      val sum =Tools.toSum(elem)
      if(sum!=0) {
        val staycount= sumpois.length
        val dinningnum = elem._1;
        val containsdinning=sumpois.filter(x=> !x._2._1.equals(0)).length;
        val dinning =Tools.TFIDF(sum,dinningnum,staycount,containsdinning);
        val hotelnum = elem._2;
        val containshotel=sumpois.filter(x=> !x._2._2.equals(0)).length;
        val hotel =Tools.TFIDF(sum,hotelnum,staycount,containshotel);
        val shoppingnum =elem._3;
        val containsshop =sumpois.filter(x=> !x._2._3.equals(0)).length;
        val shopping =Tools.TFIDF(sum,shoppingnum,staycount,containsshop);
        val spotnum =elem._4;
        val containsspot =sumpois.filter(x=> !x._2._4.equals(0)).length;
        val spot=Tools.TFIDF(sum,spotnum,staycount,containsspot);
        val sportnum =elem._5;
        val containssport =sumpois.filter(x=> !x._2._5.equals(0)).length;
        val sport=Tools.TFIDF(sum,sportnum,staycount,containssport);
        val recreationnum =elem._6;
        val containsrecreate =sumpois.filter(x=> !x._2._6.equals(0)).length;
        val recreation=Tools.TFIDF(sum,recreationnum,staycount,containsrecreate);
        val medicalnum =elem._7;
        val containsmedical =sumpois.filter(x=> !x._2._7.equals(0)).length;
        val medical=Tools.TFIDF(sum,medicalnum,staycount,containsmedical);
        val transportnum =elem._4;
        val containstransport =sumpois.filter(x=> !x._2._8.equals(0)).length;
        val transportation=Tools.TFIDF(sum,transportnum,staycount,containstransport);
        val staypooint = new Staypoint(stayp,hotel,recreation,sport,dinning,
          shopping,medical,transportation,spot)
        staypooint
      }else{
        val staypooint = new Staypoint(stayp,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1000.0)
        staypooint
      }
    }else{
      val staypooint = new Staypoint(stayp,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0)
      staypooint
    }
  }
  /**
   * 得到用户的活动类型推测
   * @param Pois (name,address,lat,lgn,uid,distance,tpe,tag,price,shophours,overrating,taste,service,environment,facility,hygiene,technology,comment)
   * @param stay_point RDD[user,home,stoptime,stopci,duration,translgn,translat,radius,ifurban,ifouter]
   * @param CellTrans （region,ifurban,ifouter,ci,bscid,jizhanid,cellid,cellname,angle,lgn,lat,radius,translgn,translat）
   * @return user+","+home+","+work+","+stoptime+","+stopci+","+duration+","+translgn+","+tranlat+","+activity
   */
  def GetActivity(Pois:RDD[String],stay_point:RDD[String],CellTrans:RDD[String]):RDD[String]={
    val cellinfo  = CellTrans.map(_.split(",",-1))
      .filter(_.length==14)//过滤掉不符合的基站信息
      .map{x=> x match{
      case Array(region,ifurban,ifouter,ci,bscid,jizhanid,
      cellid,cellname,angle,lgn,lat,radius,translgn,translat)
      => (ci,(translgn+","+translat+","+radius))}}.collect().toMap//将基站信息变成MAP类型，键值为cellid
    val pois =Pois.map(_.split("\\|",-1))
      .filter(_.length==18)//过滤掉区分不出来的错误数据
      .map{x=> x match{
      case Array(name,address,lat,lgn,uid,distance,tpe,tag,price,shophours,
      overrating,taste,service,environment,facility,hygiene,technology,comment)
      =>  (name,address,lat,lgn,uid,distance,tpe,tag,price,shophours,
        overrating,taste,service,environment,facility,hygiene,technology,comment)}}
        .collect()//因为Spark不允许RDD内在此对RDD操作，所以将poi信息转换为Array
    val stay =  stay_point.map(_.split(",",-1)).map{x=> x match{
       case Array(user,homecell,workcell,stoptime,stopci,duration,
       translgn,translat,radius,ifurban,ifouter)
       =>((user,homecell,workcell),(stoptime,duration,stopci,translgn,translat))}}
       .groupByKey()//将每个用户的停留点形成序列
       .map{x => val user= x._1._1;val homecell=x._1._2; val workcell=x._1._3;
       val staypoint =x._2.toArray.sortWith((a,b)=>(a._2<b._2));//停留点按序排序
       val distinctedstay= staypoint.map{x=>x._3 }.distinct//只保留用户的停留点并去重
       val distincted_arr = ArrayBuffer[String]()
       for(i<- 0 until distinctedstay.length){
         if(!(distinctedstay(i).equals(homecell)||distinctedstay(i).equals(workcell))){
           distincted_arr += distinctedstay(i)
         }//得到某个用户的不重复的除去家和工作地点的停留点
       }
       val activity_points =ArrayBuffer[(String,String,String,String,String,String)]()
       for(k<-0 until staypoint.length){//遍历停留点序列
         val stoptime =staypoint(k)._1;val duration =staypoint(k)._2;val stopci =staypoint(k)._3;
         val translgn =staypoint(k)._4;val tranlat=staypoint(k)._5;
        val activity=if(!translgn.equals("None")) {//跳过基站列表中没有的基站ID
          if (stopci.equals(homecell)) {
            "home"
          } else if (stopci.equals(workcell)) {
            "work"
          } else {
            val poiscore_k = GetUserPoisScores(pois,cellinfo,(stopci, distincted_arr.toArray))
            if (poiscore_k.spot.equals(1000.0)) {
              "microtravel"//判定若POI信息无则定位微旅游
            }else{
            val activity_dinning = distribution.getDinningFactor(stoptime, duration)
            val activity_shopping = distribution.getShoppingFactor(stoptime, duration)
            val activity_sport = distribution.getSportFactor(stoptime, duration)
            val activity_spot = distribution.getSpotFactor(stoptime, duration)
            val activity_recreate = distribution.getRecreationFactor(stoptime, duration)
            val activity_medical = distribution.getMedicalFactor(stoptime,duration)
            val activity_transfer = if (k + 1 < staypoint.length) {
              if (!staypoint(k + 1)._4.equals("None")) {
                val lng = staypoint(k)._4.toDouble;
                val lat = staypoint(k)._5.toDouble;
                val lng_next = staypoint(k + 1)._4.toDouble;
                val lat_next = staypoint(k + 1)._5.toDouble;
                if (Tools.GetDistance(lng, lat, lng_next, lat_next) > 40.0) {
                  5.0//判断交通中转的条件是该点与下一点距离超过40公里
                } else {
                  1.0
                }
              } else {
                1.0
              }
            } else {
              1.0
            }
            val activity_hotel = if (staypoint(k)._1.substring(8, 10).toDouble > 19.0
             && (staypoint(k)._1.substring(8, 10).toDouble + staypoint(k)._2.toDouble > 23.0)) {
              4.0
            } else {
              1.0
            }
            val result = ArrayBuffer[(String,Double)]()
            val dinningfactor = poiscore_k.dinning * activity_dinning;
            val shoppingfactor = poiscore_k.shopping * activity_shopping;
            val sportfactor = poiscore_k.sports * activity_sport;
            val spotfactor = poiscore_k.spot * activity_spot;
            val recreatefactor = poiscore_k.recreation * activity_recreate;
            val medicalfactor = poiscore_k.medical * activity_medical;
            val transferfactor = poiscore_k.transportation * activity_transfer;
            val hotelfactor = poiscore_k.hotel * activity_hotel;
            val sum = dinningfactor + shoppingfactor + sportfactor + spotfactor +
             recreatefactor + medicalfactor + transferfactor + hotelfactor
            result += (("dinning", dinningfactor / sum));
            result += (("shopping", shoppingfactor / sum));
            result += (("sport", sportfactor / sum));
            result += (("spot", shoppingfactor / sum));
            result += (("recreation", recreatefactor / sum));
            result += (("medical", medicalfactor / sum));
            result += (("transfer", transferfactor / sum));
            result += (("hotel", hotelfactor / sum))
            val max_probability = result.sortWith((a, b) => (a._2 > b._2))(0)._1//对最后的每项得分进行排序取最大值
            max_probability
          }
        }
        }else{
          "undefined"
        }
         activity_points += ((stoptime,stopci,duration,translgn,tranlat,activity))
       }
       (user+","+homecell+","+workcell,activity_points.toArray)
     }.flatMapValues(x=>x).map{x=> x match{
        case (user,(stoptime,stopci,duration,translgn,tranlat,activity))
        => user+","+stoptime+","+stopci+","+duration+","+translgn+","+tranlat+","+activity}}
    stay
  }
}
