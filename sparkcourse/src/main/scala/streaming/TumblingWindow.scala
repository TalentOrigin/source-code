package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object TumblingWindow {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val retailDataSchema = new StructType()
      .add("InvoiceNo", IntegerType)
      .add("StockCode", IntegerType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", DateType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", IntegerType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    val streamData = spark.readStream
      .schema(retailDataSchema)
      .option("maxFilesPerTrigger","2")
      .csv("/Users/talentorigin/temp_working")

    val tumblingWindowAggregations = streamData
        .filter("Country = 'Spain'")
      .groupBy(
        window(col("InvoiceTimestamp"), "1 hours"),
        col("Country")
      )
      .agg(sum(col("UnitPrice")))

    val sink = tumblingWindowAggregations
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .start()

    sink.awaitTermination()
  }
}
