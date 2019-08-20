package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}

object StreamingTransformations {
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

    val filteredData = streamingData
      .filter("Country = 'France'")
      .where("Quantity > 1")
      .drop("CustomerId")
      .select("InvoiceNo","StockCode", "Description","Quantity")
      .writeStream
      .queryName("SalesDetails")
      .format("console")
      .outputMode(OutputMode.Update())
      .start()

    filteredData.awaitTermination()

  }
}
