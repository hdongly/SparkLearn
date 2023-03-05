package com.apple.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}

object Play002 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark
      .read
      .option("header", true).option("delimiter", " ").csv("datas/girls").createOrReplaceTempView("user")
    val udaf = new MyAvgAgeUDAF

    spark.udf.register("avgAge", functions.udaf(udaf))

    spark.sql("select name, avgAge(score) from user group by name").show

    spark.stop()
  }
}
