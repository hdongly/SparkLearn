import com.apple.bean.Girl
import com.apple.udf.MyAvgAgeUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}

object Play002 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    spark
      .read
      .option("header", true)
      .option("delimiter", " ")
      .csv("datas/girls")
      .createOrReplaceTempView("user")


    spark
      .sql("select *, named_struct('name','nju','major','gis') school from user")
      .as[Girl]
      .repartitionByRange()
      .show()

    spark.stop()
  }
}
