import com.apple.bean.{Girl, School}

object Play003 {
  def main(args: Array[String]): Unit = {
    val myj = Girl("myj", "18", "beauty", School("nju", "gis"))

    println(myj.showInfo)
  }

  private def mergeValues[V](vs1: Map[String, Option[V]], vs2: Map[String, Option[V]]): Map[String, Option[V]] = {
    vs1.filter { case (_, v) => v.isDefined } ++ vs2.filter { case (_, v) => v.isDefined }
  }
}
