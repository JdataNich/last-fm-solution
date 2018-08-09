import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object MyFirstObject {
  def main (args:Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("test").master("local")
      .getOrCreate()

    val path = "/home/joe/Downloads/lastfm-dataset-1K"

    val inputData = spark
      .read.format("csv")
      .option("sep", "\t").load(path)

    val newSchema = Seq("userid", "timestamp", "artistid", "artistname", "trackid", "trackname")

    val lastFMData = inputData.toDF(newSchema: _*)

    val usersDistinctSongCounts = lastFMData.groupBy("userid")
      .agg(countDistinct("artistname", "trackname"))

    val songsTotalPlayCounts = lastFMData
      .groupBy("artistname", "trackname")
      .agg(count( "*").as("playCount"))

    val topSongs =  songsTotalPlayCounts.orderBy(songsTotalPlayCounts("playCount").desc)
      .limit(100)

    usersDistinctSongCounts.show(10)

    topSongs.show(10)

  }
}
