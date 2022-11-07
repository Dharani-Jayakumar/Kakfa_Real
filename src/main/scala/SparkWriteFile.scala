import org.apache.spark.sql.{SparkSession, DataFrame}


object SparkWriteFile {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkWriteFile")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
      //read the url
      val url_toread = requests.get("https://data-live.flightradar24.com/zones/fcgi/feed.js?bounds=59.09,52.64,-58.77,-47.71&faa=1&mlat=1&flarm=1&adsb=1&gnd=1&air=1&vehicles=1&estimated=1&maxage=7200&gliders=1&stats=1")
      val total = url_toread.text()
      import spark.implicits._
      val dfFromText = spark.read.json(Seq(total).toDS)
      dfFromText.write.json("C:\\SparkWriteExample\\new\\")

    }


  }
