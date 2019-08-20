import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions.{approx_count_distinct}


object MyFirstSparkJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Spark Structured Streaming").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val storeSchema = new StructType()
      .add("invoiceNo", IntegerType)
      .add("stockCode", IntegerType)
      .add("description", StringType)
      .add("quantity", IntegerType)
      .add("invoiceDate", TimestampType)
      .add("unitPrice", DoubleType)
      .add("customerId", IntegerType)
      .add("country", StringType)

    val streamData = spark.readStream
      .schema(storeSchema)
      .option("maxFilesPerTrigger", "5")
      .csv("/Users/talentorigin/temp_working")

    val invoiceCounts = streamData.select("invoiceNo", "stockCode", "quantity", "unitPrice", "country")
      .filter("country == 'United Kingdom' AND invoiceNo == 536575")
      .groupBy("invoiceNo")
      .agg(approx_count_distinct("stockCode"))

    val invoiceCountQuery = invoiceCounts
      .writeStream
      .queryName("invoiceCounts")
      .format("console")
      .outputMode("complete")
      .start()

    invoiceCountQuery.awaitTermination()
    spark.stop()
  }
}
