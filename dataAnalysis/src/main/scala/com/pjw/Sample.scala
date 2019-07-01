package com.pjw

import org.apache.spark.{SparkConf, SparkContext}

object Sample {
  def main(args: Array[String]): Unit = {
    var a = 10
    var b = 6
    var c = if(b>5){a}else{b}

    var intValue = 10
    var doubleValue = 3.0

    //plus
    var sumValue = intValue + doubleValue
    print(c)
  }
}
