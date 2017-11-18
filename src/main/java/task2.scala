import org.apache.spark.sql.{DataFrame, SparkSession}

object task2 {
  def saveDF(df: DataFrame, output: String): Unit = {

    df.repartition(1)
      .write
      .option("delimiter", "\t")
      .csv(output)
  }
  def main(args: Array[String]): Unit = {

    val gdeltFile = args(0)
    val cameoFile = args(1)

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("SparkSQLDataFrames")
      .master("local[*]")
      .getOrCreate()

    //Initialize GDELT DataFrame
    val gdeltDF = new GDELTdata(sparkSession, gdeltFile, cameoFile)

    //gdeltDF.gdelt.show(100)

    gdeltDF.gdelt.printSchema()
    gdeltDF.gdeltSchema.printTreeString()
    //val gd=gdeltDF.gdelt


    val gdeltUSA = gdeltDF.readCountryDataFrame("'US'")
    val gdeltRussia=gdeltDF.readCountryDataFrame("'RS'")



    //Join with cameo codes dictionary
  /*  val cameoCodes = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(cameoFile)*/


    val cameoCodes=gdeltDF.Cameo.toDF()

    cameoCodes.show()

    gdeltUSA.join(cameoCodes,
      gdeltUSA.col("EventCode") === cameoCodes.col("CAMEOcode"), "inner")
      .select("GLOBALEVENTID", "EventCode", "EventDescription", "ActionGeo_CountryCode", "ActionGeo_Lat", "ActionGeo_Long")
      .show()

    gdeltRussia.join(cameoCodes,
      gdeltRussia.col("EventCode") === cameoCodes.col("CAMEOcode"), "inner")
      .select("GLOBALEVENTID", "EventCode", "EventDescription", "ActionGeo_CountryCode", "ActionGeo_Lat", "ActionGeo_Long")
      .show()


    //2
    val gdeltAct1=gdeltDF.Actors1
    gdeltAct1.show(200)
    gdeltAct1.createOrReplaceTempView("Actors1")
    sparkSession.sql(
      "SELECT first(Actor1Name), COUNT(*) as cnt" +
        " FROM Actors1" +
        " GROUP BY Actor1Name" +
        " ORDER BY cnt DESC LIMIT 10")
      .show(100)

    //4
    val gdeltEvents=gdeltDF.readEvents
    gdeltEvents.show(50)
   /* gdeltEvents.createOrReplaceTempView("Events")
    cameoCodes.createOrReplaceTempView("Codes")
    sparkSession.sql(
      "SELECT first(Events.EventCode), Codes.EventDescription, COUNT(*) as cnt" +
        " FROM Events" +
        " INNER JOIN Codes ON Events.EventCode=Codes.CAMEOcode" +
        " GROUP BY Events.EventCode" +
        " ORDER BY cnt DESC LIMIT 10")
      .show(100)*/


    val Ev=gdeltEvents.join(cameoCodes,
      gdeltEvents.col("EventCode")===cameoCodes.col("CAMEOcode"), "inner")
      .select("GLOBALEVENTID", "EventCode", "EventDescription")

    Ev.show(100)
    Ev.createOrReplaceTempView("Events")
    sparkSession.sql(
      "SELECT first(EventCode), first(EventDescription), COUNT(*) as cnt" +
        " FROM Events" +
        " GROUP BY EventCode" +
        " ORDER BY cnt DESC LIMIT 10")
      .show(100)









  }
}





