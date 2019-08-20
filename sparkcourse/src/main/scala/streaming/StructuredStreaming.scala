package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}

object StructuredStreaming {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val retailDataSchema = new StructType()
      .add("InvoiceNo", IntegerType)
      .add("StockCode", IntegerType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", TimestampType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", IntegerType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    val streamingData = spark
      .readStream
      .schema(retailDataSchema)
      .csv("/Users/talentorigin/temp_working")

    val filteredData = streamingData.filter("Country = 'United Kingdom'")

    val query = filteredData.writeStream
      .format("console")
      .queryName("filteredByCountry")
      .outputMode(OutputMode.Update())
      .start()

    query.awaitTermination()
  }
}
