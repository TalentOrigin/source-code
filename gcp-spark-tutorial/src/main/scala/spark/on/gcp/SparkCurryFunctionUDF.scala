package spark.on.gcp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object SparkCurryFunctionUDF {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder()
      .appName("Spark UDF Using Curry Functions")
      .getOrCreate()

    val data = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(args(0))

    val precision = args(1).toInt

    data.show()

    val diffData = data.withColumn("DIFFCLOSE", diffCalculation(precision)(data("CLOSE"), data("PREVCLOSE")) )
    diffData.show()

  }

  def diffCalculation(precision: Int): UserDefinedFunction = udf( (close: Double, prevClose: Double) => {
    BigDecimal(close - prevClose).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
  })

}
