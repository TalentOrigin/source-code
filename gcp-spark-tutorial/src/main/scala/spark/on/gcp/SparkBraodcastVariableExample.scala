package spark.on.gcp

import org.apache.spark.sql.SparkSession

object SparkBraodcastVariableExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder()
      .appName("Broadcast variable demo")
      .getOrCreate()

    val users = spark.sparkContext.textFile(args(0))
    val songs = spark.sparkContext.textFile(args(1))
    val userSongPlayCount = spark.sparkContext.textFile(args(2))

    val usersMap = users.zipWithIndex().map(user => (user._1, user._2)).collectAsMap()
    val songsMap = songs.map(song => song.split(" ")).map(songarr => (songarr(0), songarr(1)))
      .collectAsMap()
    val userSongCountArray = userSongPlayCount.map(ele => ele.split("\t"))

    val usersBroadcast = spark.sparkContext.broadcast(usersMap)
    val songsBroadcast = spark.sparkContext.broadcast(songsMap)

    val modifiedCounts = userSongCountArray.map {
      case Array(uid, sid, count) =>
        val user = usersBroadcast.value.getOrElse(uid, 0)
        val song = songsBroadcast.value.getOrElse(sid, 0)

        (user, song, count)
    }


    modifiedCounts.take(50).foreach(println)
  }
}
