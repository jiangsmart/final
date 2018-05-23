package wtist.activityPattern.impl

/**
 * Created by Pray 
 * Date on 2016/5/5.
 */
class Staypoint(val stay:String,val hote:Double ,val recre:Double,val sport:Double,val din:Double, val shop:Double,val medic:Double,val trans:Double,val sp:Double) {
        var staypoint :String = stay;
        var hotel:Double =hote;
        var recreation:Double =recre;
        var sports:Double =sport;
        var dinning:Double =din;
        var shopping:Double =shop;
//        var lodging:Double =0.0;
         var medical:Double=medic;
        var transportation:Double = trans;
        var spot:Double=sp;
        //(美食，酒店数，购物中心数，景点数，运动健身，休闲，医疗，交通)
  override def toString():String=staypoint+","+dinning.toString+","+hotel.toString+","+shopping.toString+","+spot.toString+","+sports.toString+","+recreation.toString+","+medical.toString+","+transportation.toString

}
