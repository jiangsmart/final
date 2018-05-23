package wtist.activityPattern.impl

/**
 * Created by Pray 
 * Date on 2016/5/18.
 */
object distribution {

  /**
   * 二次函数
   * @param a
   * @param b
   * @param c
   * @param x
   * @return
   */
  def quadraticFunctions(a:Double,b:Double,c:Double,x:Double):Double={
   a*Math.pow(x,2)+b*x-c
  }

  /**
   * 得到餐饮的吸引系数
   * @param time
   * @param duration
   * @return
   */
  def getDinningFactor(time:String,duration:String):Double={
   val hour = time.substring(8,10).toDouble
    var factor =0.0;
    if(duration.toDouble<3.0){
   if(hour<10.0 && hour>6.0){
//     factor = -1.25*Math.pow(hour,2)+21.25*hour-81.3125
        factor = quadraticFunctions(-1.25,21.25,81.3125,hour)
   }else if (hour>=10.0 && hour<=14.0){
//     factor = -1.25*Math.pow((hour-4.5),2)+20*(hour-4.5)-75
     factor = quadraticFunctions(-1.25,20,75,hour-4.5)
   }else if(hour<=20 && hour>=17){
     factor = quadraticFunctions(-1.25,20,75,hour-11.5)
//      factor= -1.25*Math.pow((hour-11.5),2)+20*(hour-11.5)-75
   }else{
     factor =0.5;
   }}else{
      factor=0.5
    }
  factor
  }

  /**
   * 得到shopping系数
   * @param time
   * @param duration
   * @return
   */
  def getShoppingFactor(time:String,duration:String):Double={
    val hour = time.substring(8,10).toDouble
    var factor =0.0;
    if(hour<=17.5 && hour>=14.5){
      factor = quadraticFunctions(-1,32,252,hour)
    }else if(hour<=21 && hour>=20){
//     factor= -4*Math.pow(hour,2)+164*hour-1678
      factor = quadraticFunctions(-4,164,1678,hour)
    }else{
      factor =1.0;
    }
    factor
  }

  /**
   * 得到运动健身系数
   * @param time
   * @param duration
   * @return
   */
  def getSportFactor(time:String,duration:String):Double={
    val hour = time.substring(8,10).toDouble
    var factor =0.0;
    if(hour<11.0 && hour>9.0){
      factor = quadraticFunctions(-1,20,97.5,hour)
    }else if (hour>=14.0 && hour<=17.0){
      factor = quadraticFunctions(-4,124,957,hour)
    }else if(hour<=21 && hour>=19){
      factor = 0.8*quadraticFunctions(-4,164,1678,hour-11.5)
    }else{
      factor =1.0;
    }
    factor
  }
  /**
   * 得到休闲娱乐系数
   * @param time
   * @param duration
   * @return
   */
  def getRecreationFactor(time:String,duration:String):Double={
    val hour = time.substring(8,10).toDouble
    var factor =0.0;
    if(hour<11.0 && hour>9.0){
      factor = quadraticFunctions(-1,20,97.5,hour)
    }else if (hour>=14.0 && hour<=17.0){
      factor = quadraticFunctions(-4,124,957,hour)
    }else if(hour<=22 && hour>=20){
      factor = quadraticFunctions(-1,42,436,hour)
    }else if(hour>22){
        factor=3.5
    }else{
      factor =1.0;
    }
    factor
  }

  /**
   * 得到医疗系数
   * @param time
   * @param duration
   * @return
   */
  def getMedicalFactor(time:String,duration:String):Double={
    val hour = time.substring(8,10).toDouble
    var factor =0.0;
    if(hour<=17.5 && hour>=14.5){
      factor = 2.0
    }else if(hour<=11 && hour>=8){
      //     factor= -4*Math.pow(hour,2)+164*hour-1678
      factor = 2.0
    }else{
      factor =1.0;
    }
   factor
  }

  /**
   * 得到景点分布
   * @param time
   * @param duration
   * @return
   */
  def getSpotFactor(time:String ,duration:String ):Double={
    val hour = time.substring(8,10).toDouble
    var factor =0.0;
    if(hour<=17.5 && hour>=14.5){
      factor = quadraticFunctions(-1,32,252,hour)
    }else if(hour<=11 && hour>=9){
      //     factor= -4*Math.pow(hour,2)+164*hour-1678
      factor = quadraticFunctions(-1,20,97,hour)
    }else{
      factor =1.0;
    }
    factor
  }
}
