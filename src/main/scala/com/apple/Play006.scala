import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Play003 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

//    spark.read.json("/Users/hdong/Downloads/a.json").select("description", "displayName", "name", "short", "suffix", "tags", "unit").createOrReplaceTempView("EIView")
//    spark.read.json("/Users/hdong/Downloads/b.json").createOrReplaceTempView("VDASHView")
//
//    spark.sql("select * from VDASHView a anti join EIView b on a.description<=>b.description and a.displayName<=>b.displayName and a.name<=>b.name and a.short<=>b.short and a.suffix<=>b.suffix and a.tags<=>b.tags and a.unit<=>b.unit").show(100, false)
    val articles = Seq(
      Article("Gergely", "Spark joins", 0),
      Article("Kris", "Athena with DataGrip", 1),
      Article("Kris", "Something Else", 2),
      Article("Gergely", "My article in preparation", 3)
    ).toDS

    val views = Seq(
      ArticleView(0, 1),   //my article is not very popular :(
      ArticleView(1, 123), //Kris' article has been viewed 123 times
      ArticleView(2, 24),  //Kris' not so popular article
      ArticleView(10, 104) //a deleted article
    ).toDS

    articles
      .joinWith(views, articles("id") === views("articleId"), "inner")
//      .map{ case (a,v) => AuthorViews(a.author, v.viewCount) }
      .map {
        case (a, null) => AuthorViews(a.author, 0)
        case (null, v) => AuthorViews("Unknown article", v.viewCount)
        case (a, v) => AuthorViews(a.author, v.viewCount)
      }
      .show(100, false)

    spark.stop()
  }
}

case class Article(author: String, title: String, id: Int)
case class ArticleView(articleId: Int, viewCount: Int)
case class AuthorViews(author: String, viewCount: Int)