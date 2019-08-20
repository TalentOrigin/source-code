package spark.on.gcp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object UdfExampleApproach1 {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder()
      .appName("Spark UDF Using Curry Functions")
      .getOrCreate()

    val data = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/Users/talentorigin/workspace/datasets/nse_stocks/nse-stocks-data.csv")

    val diffCalculationUDF = udf[Double, Double, Double](diffCalculation)

    val diffColumnData = data.withColumn("DIFFCLOSE",diffCalculationUDF(data("PREVCLOSE"), data("CLOSE")))
    diffColumnData.show()
  }

  def diffCalculation(num1: Double, num2: Double): Double = num1 - num2
}
