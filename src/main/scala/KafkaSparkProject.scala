import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.{DataFrame, SparkSession}
object KafkaSparkProject  {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    while (true) {
//      val url_toread = requests.get("https://data-live.flightradar24.com/zones/fcgi/feed.js?bounds=59.09,52.64,-58.77,-47.71&faa=1&mlat=1&flarm=1&adsb=1&gnd=1&air=1&vehicles=1&estimated=1&maxage=7200&gliders=1&stats=1")
val url_toread = requests.get("https://data-live.flightradar24.com/zones/fcgi/feed.js?bounds=59.09,52.64,-58.77,-47.71")

      val total = url_toread.text()
      println(total)

      import spark.implicits._
      val dfFromText: DataFrame = spark.read.json(Seq(total).toDS)
      //println(dfFromText.printSchema())
//      println(dfFromText.select($"stats").toJSON.show(false))
      dfFromText.show()
//      val kafka_msg = dfFromText.select(to_json(struct($"stats"))).toDF("value")
      val kafka_msg = dfFromText.select(to_json(struct($"full_count"))).toDF("value")
      println(kafka_msg.show(false))
//        kafka_msg.selectExpr("value")
//        .write
//        .format("kafka")
//        .option("kafka.bootstrap.servers", "master1.internal.cloudapp.net:9092")
//        .option("topic", "dharani_topic")
//        .save()

//val lines=spark.readstream.format("kafka").option("kafka.bootstrap.servers",conf.getstring("bootstrap.servers")).option("subscribe","gabi").load.select("value",cast("string").alias("value"))
//
//      import org.apache.spark.sql.streaming.Trigger
//      lines.writestream.format("console").outputmode("append").trigger("Trigger.ProcessingTime("20 seconds")).start

      Thread.sleep(120000)
    }


  }


}
