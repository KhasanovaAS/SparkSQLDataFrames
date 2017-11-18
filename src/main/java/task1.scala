import org.apache.spark.sql.SparkSession

object TestScala3 {
  def main(args: Array[String]): Unit = {

    val jsonFile = args(0);

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("SparkSQLDataFrames")
      .master("local[*]")
      .getOrCreate()

    //Read json file to DF
    val tweetsDF = sparkSession.read.json(jsonFile)

    //Show the first 100 rows
    tweetsDF.show(100);


    //Show thw scheme of DF
    tweetsDF.printSchema();

    // Register the DataFrame as a SQL temporary view
    tweetsDF.createOrReplaceTempView("tweetTable")
    //Get the actor name and body of messages in Russian
    sparkSession.sql(
      " SELECT actor.displayName, object.twitter_lang, body" +
        " FROM tweetTable WHERE body IS NOT NULL" +
        " AND object.twitter_lang = 'ru'")
      .show(100)

    //Find how many tweets each user has
    sparkSession.sql(
      "SELECT first(actor.displayName), COUNT(*) as cnt" +
        " FROM tweetTable" +
        " WHERE body IS NOT NULL" +
        " GROUP BY actor.id" +
        " ORDER BY cnt DESC")
      .show(100)

    //Find the 10 most mentioned persons
    sparkSession.sql(
      " SELECT first(twitter_entities.user_mentions.name) as name, COUNT(*) as cnt" +
        " FROM tweetTable " +
        " WHERE  body IS NOT NULL" +
        " GROUP BY twitter_entities.user_mentions.id ORDER BY cnt DESC LIMIT 10")
      .show(100)

    //Find all the hashtags mentioned on a tweet
    sparkSession.sql(
      " SELECT distinct twitter_entities.hashtags.text as hashtags" +
        " FROM tweetTable " +
        " WHERE  twitter_entities.hashtags.text IS NOT NULL")
      .show(100)

    //Count how many times each hashtag is mentioned
    sparkSession.sql(
      " SELECT first(twitter_entities.hashtags.text) as name, COUNT(*) as cnt" +
        " FROM tweetTable " +
        " WHERE  twitter_entities.hashtags.text IS NOT NULL" +
        " GROUP BY twitter_entities.hashtags.text ORDER BY cnt DESC")
      .show(100)

    //Find the 10 most popular Hashtags
    sparkSession.sql(
      " SELECT first(twitter_entities.hashtags.text) as name, COUNT(*) as cnt" +
        " FROM tweetTable " +
        " WHERE  twitter_entities.hashtags.text IS NOT NULL" +
        " GROUP BY twitter_entities.hashtags.text ORDER BY cnt DESC LIMIT 10")
      .show(100)

 }
}
