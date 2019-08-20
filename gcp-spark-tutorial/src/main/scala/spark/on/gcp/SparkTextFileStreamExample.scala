package spark.on.gcp

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkTextFileStreamExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark DStream API text file stream example")

    val ssc = new StreamingContext(sparkConf, Seconds(15))

    val streamRDD = ssc.textFileStream("/Users/talentorigin/Downloads/temp_working/")

    val filteredData = streamRDD.filter(!_.matches("\\w+"))

    val groupedData = filteredData.map{
      row =>
        val rowElements = row.split(",")
        (rowElements(0), row)
    }.groupByKey.count()

    groupedData.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
