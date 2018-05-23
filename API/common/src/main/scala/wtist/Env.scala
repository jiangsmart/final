package wtist

import org.apache.spark.{SparkConf, SparkContext}
import wtist.Tools.{isOdd, isZero}

/**
 * Created by wtist on 2016/5/12.
 */
object Env {
    var sc: SparkContext = null

    /**
     * 输入参数检查
     * @param args 参数
     * @param Usage 用法打印
     * @param IsEven 参数个数是否为偶数
     * @param IsNonZero 参数个数是否为0
     */
    def check(args: Array[String], Usage: String, IsEven: Boolean = true, IsNonZero: Boolean = true): Unit = {
        if(isZero(args.length) == IsNonZero || isOdd(args.length) == IsEven) {
            println("Erro: the input parameters is error")
            println(Usage)
            System.exit(-1);
        }
    }

    /**
     * 初始化SparkContext
     * @param args 参数列表
     * @param AppName app名称
     * @return SparkContext
     */
    def init(args: Array[String], AppName: String): SparkContext = {
        Arguments.push(args)
        sc = new SparkContext(new SparkConf().setAppName(AppName))
        sc
    }
    val SPACE = " ";//空格
    val COMMA = ",";//逗号
    val SEMICOLON = ";";//分号
    val WELL = "#";//#号
    val AND = "&";//&符号
    val EQUAL = "=";//等号
    val VerticalLine = "|";//竖线
    val Asterisk = "*";//星号
    val Tab = "\t";//tab键
    val LineBreak = "\n";//换行符
    val timeDefaultRegex = "\t"
}
