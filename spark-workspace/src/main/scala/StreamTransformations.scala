import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._

object StreamTransformations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(name = "Spark Streaming Transformations")
      .master(master = "local")
      .getOrCreate()

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

    val retailDataStream = spark.readStream
      .schema(retailDataSchema)
      .option("maxFilesPerTrigger", "5")
      .option("header","true")
      .csv("/Users/talentorigin/temp_working")

    val query = retailDataStream.filter("Country = 'United Kingdom'")

    val processStream = query
      .writeStream
      .format("console")
      .outputMode("append")
      .start()

    processStream.awaitTermination()
  }
}
