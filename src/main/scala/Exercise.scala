import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
//import spark.implicits._
object Exercise {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    // sc is an existing SparkContext.
//    val sc = new SparkContext(conf)
//    val df = {
//  spark.read.seq((2012, 8, "Batman", 9.8), (2012, 8, "Hero", 8.7), (2012, 7, "Robot", 5.5), (2011, 7, "Git", 2.0)).toDF("year", "month", "title", "rating")
//}

  }
}