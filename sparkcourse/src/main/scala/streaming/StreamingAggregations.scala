package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object StreamingAggregations {
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

    val aggregateData = streamData
      .filter("Quantity > 10")
      .groupBy("InvoiceDate", "Country")
      .agg(sum("UnitPrice"))

      val aggregateQuery = aggregateData.writeStream
        .format("console") // represents sink to be used
        .outputMode(OutputMode.Append())
        .start()

    aggregateQuery.awaitTermination()

  }
}
