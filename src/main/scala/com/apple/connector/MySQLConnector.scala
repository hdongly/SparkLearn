package com.apple.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MySQLConnector {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/Playground")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "Lovely1106*")
      .option("dbtable", "test01")
      .load().show

    spark.stop()
  }
}
