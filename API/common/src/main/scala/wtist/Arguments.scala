package wtist
import scala.collection.mutable.ArrayBuffer

/**
 * Created by wtist on 2016/5/25.
 */
class Arguments extends Serializable {
  val inputs: ArrayBuffer[String] = new ArrayBuffer[String]
  val outputs: ArrayBuffer[String] = new ArrayBuffer[String]
  val params: ArrayBuffer[String] = new ArrayBuffer[String]

  def push(args: Array[String]) {
    var tmp = inputs
    for (arg <- args) {
      if (arg.equals("-i") || arg.equals("-input")) {
        tmp = inputs
      } else if (arg.equals("-o") || arg.equals("-output")) {
        tmp = outputs
      } else if (arg.equals("-p") || arg.equals("-para")) {
        tmp = params
      } else {
        tmp += arg
      }
    }
  }

  def getInputs(): Array[String] = {
    return inputs.toArray
  }

  def getOutputs(): Array[String] = {
    return outputs.toArray
  }

  def getParams(): Array[String] = {
    return params.toArray
  }
}

object Arguments extends Serializable {
  val arg: Arguments = new Arguments

  def push(args: Array[String]) = arg.push(args: Array[String])

  def getInputs = arg.getInputs

  def getOutputs = arg.getOutputs

  def getParams = arg.getParams

  def getInputByIndex(index: Int): String = {
      getInputs(index)
  }

  def getOutputByIndex(index: Int): String = {
      getOutputs(index)
  }

  def getParamAsInt(index: Int): Int = {
    getParams(index).toInt
  }

  def getParamAsInt(index: Int, default: Int): Int = {
    if (arg.getParams() == null || arg.getParams().length <= index) {
      default
    } else {
      getParams(index).toInt
    }
  }

  def getParamAsDouble(index: Int): Double = {
    getParams(index).toDouble
  }

  def getParamAsDouble(index: Int, default: Double): Double = {
    if (arg.getParams() == null || arg.getParams().length <= index) {
      default
    } else {
      getParams(index).toDouble
    }
  }

  def getParamAsString(index: Int): String = {
    getParams(index)
  }

  def getParamAsString(index: Int, default: String): String = {
    if (arg.getParams() == null || arg.getParams().length <= index) {
      default
    } else {
      getParams(index)
    }
  }

  def getParamAsBoolean(index: Int): Boolean = {
    getParams(index).toBoolean
  }

  def getParamAsBoolean(index: Int, default: Boolean): Boolean = {
    if (arg.getParams() == null || arg.getParams().length <= index) {
      default
    } else {
      getParams(index).toBoolean
    }
  }
  def printArguments(): Unit ={
    for (input <- arg.getInputs()) {
      println("i:" + input)
    }
    for (output <- arg.getOutputs()) {
      println("o:" + output)
    }
    for (output <- arg.getParams()) {
      println("p:" + output)
    }
  }

  def main(args: Array[String]) {
    val tmp = new Array[String](8)
    tmp(0) = "-i"
    tmp(1) = "input1"
    tmp(2) = "-i"
    tmp(3) = "input2"
    tmp(4) = "-i"
    tmp(5) = "input3"
    tmp(6) = "-o"
    tmp(7) = "output"
    arg.push(tmp)
    for (input <- arg.getInputs()) {
      println("i:" + input)
    }
    for (output <- arg.getOutputs()) {
      println("o:" + output)
    }
  }
}