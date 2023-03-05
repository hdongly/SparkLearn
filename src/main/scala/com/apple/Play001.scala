import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, concat_ws, count, expr, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}


object Play001 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark = SparkSession.builder().config(conf).getOrCreate()
//    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "31457280")
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

//    val rootPath: String = _
//    val hdfs_path_apply: String = s"${rootPath}/apply"
//    val applyNumbersDF = spark.read.parquet(hdfs_path_apply)
//
//    val hdfs_path_lucky: String = s"${rootPath}/lucky"
//    val luckyDogsDF = spark.read.parquet(hdfs_path_lucky)
//
//    val filteredLuckyDogs = luckyDogsDF.filter(col("batchNum") >= "201601").select("carNum")
//
//    val jointDF = applyNumbersDF.join(filteredLuckyDogs, Seq("carNum"), "inner")
//
//    val multipliers = jointDF.groupBy(col("batchNum"), col("carNum"))
//      .agg(count(lit(1)).alias("multiplier"))
//
//    val uniqueMultipliers = multipliers.groupBy("carNum")
//      .agg(functions.max("multiplier").alias("multiplier"))
//
//    val result = uniqueMultipliers.groupBy("multiplier")
//      .agg(count(lit(1)).alias("cnt"))
//      .orderBy("multiplier")

//    val sourceDF = spark.read.option("header", true).option("delimiter", " ").csv("/Users/hdong/Downloads/GirlInfo")
//
//    sourceDF.cache()
//    sourceDF.write.mode(SaveMode.Overwrite).parquet("/Users/hdong/Downloads/SaveDatas/girls/info")

//    spark.read.parquet("/Users/hdong/Downloads/SaveDatas/girls/info").show(10, false)

//    val df = sourceDF.withColumn("age", concat_ws(",", col("age"), lit("lovely")))

    val df = spark.read.parquet("/Users/hdong/Downloads/datas/multiple-datas")

    val df2 = df
      .groupBy("customer_id")
      .agg(count(lit(1)).alias("cnt"))
//      .withColumn("cnt", expr("cast(cnt as String)"))

//    val str = sizeNew(df2, spark)
//    println(str)

    df.join(df2, Seq("customer_id"), "left").show(1, false)

    while (true) {

    }

    spark.stop()
  }

  def sizeNew(func: => DataFrame, spark: => SparkSession): String = {
    val result = func
    val lp = result.queryExecution.logical
    val size = spark.sessionState.executePlan(lp).optimizedPlan.stats.sizeInBytes
    "Estimated size: " + size / 1024 + "KB"
  }
}
