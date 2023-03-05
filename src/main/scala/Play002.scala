import org.apache.spark.sql.Row

object Play002 {
  def main(args: Array[String]): Unit = {
//    val fields = extractFields

    val ints = plus100(List(1, 2, 3))
    println(ints)
  }

  val extractFields: Seq[Row] => Seq[(String, Int)] = {
    (rows: Seq[Row]) => {
      var fields = Seq[(String, Int)]()
      rows.map(row => {
        fields = fields :+ (row.getString(2), row.getInt(4))
      })
      fields
    }
  }

  val plus100: List[Int] => List[Int] = {
    nums: List[Int] => {
      nums.map(r => r + 100)
    }
  }
}
