import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}

object StreamingTransformations {
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

    retailDataStream.groupBy{
      window(col("InvoiceTimestamp"), "60 minutes")
    }.count()
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
        .awaitTermination()

//    val selections = retailDataStream.where("Country = 'United Kingdom'")
//      .where("Quantity > 1")
//      .select("InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice")
//      .drop("CustomerId")
//
//
//    val transactionPrice = selections
//      .groupBy("InvoiceNo")
//        .agg(sum("UnitPrice").as("InvoiceTotal"))
//      .writeStream
//      .format("console")
//      .outputMode("complete")
//      .start()


//    transactionPrice.awaitTermination()
  }
}
