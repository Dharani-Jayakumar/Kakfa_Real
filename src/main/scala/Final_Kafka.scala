import org.apache.spark.sql.functions.{col, current_timestamp, date_format, from_json, struct, to_json}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//spark-shell --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7","com.lihaoyi:requests_2.11:0.7.1"

object Final_Kafka {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // while loop to take live data
    while (true) {
      //read the url
      val url_toread = requests.get("https://data-live.flightradar24.com/zones/fcgi/feed.js?bounds=59.09,52.64,-58.77,-47.71&faa=1&mlat=1&flarm=1&adsb=1&gnd=1&air=1&vehicles=1&estimated=1&maxage=7200&gliders=1&stats=1")
      val total = url_toread.text()

      //convert into dataset

      import spark.implicits._
      val dfFromText = spark.read.json(Seq(total).toDS)
      val kafka_msg: DataFrame = dfFromText.select("full_count", "stats.total.estimated", "stats.total.ads-b", "stats.total.satellite")

      //write into kafka
      kafka_msg.selectExpr("CAST(full_count AS STRING) AS key", "to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers", "master1.internal.cloudapp.net:9092").option("topic", "new").save()

      //Read from kafka
      val df = spark.read.format("kafka").option("kafka.bootstrap.servers", "master1.internal.cloudapp.net:9092").option("subscribe", "new").option("startingOffsets", "earliest").load()

      val flightStringDF = df.selectExpr("CAST(Key AS STRING) AS key", "CAST(Value AS STRING) AS value")

      val schema = new StructType().add("full_count", IntegerType).add("estimated", IntegerType).add("ads-b", IntegerType).add("satellite", IntegerType)

      val flightDF = flightStringDF.select(from_json(col("value"), schema).as("data")).select("data.*")

      val flightfinal = flightDF.withColumn("time_stamp", date_format(current_timestamp(), "y-M-d' 'H:m:s"))
      val flight1 = flightfinal.withColumn("time_stamp", col("time_stamp").cast("timestamp"))

      flight1.write.mode("append").saveAsTable("dharani.final_table")


      Thread.sleep(5000)
    }

  }
  }
