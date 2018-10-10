package com.knoldus

import org.apache.spark.sql.{ SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Power {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("PowerPlant Analysis")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val customSchema = StructType(Array(
      StructField("TimeStamp", StringType, nullable = true),
      StructField("Temperature", DoubleType, nullable = true)
    )
    )

    def getTime(timeStamp: String) = {
      val newtime = timeStamp.slice(11, 16)
      newtime
    }

    val parseTime = udf(getTime _, StringType)
    val df = spark.read.format("com.databricks.spark.csv")
      .schema(customSchema)
      .load("data.csv")

    val partition: Unit = df.withColumn("Time", parseTime(df.col("timestamp"))).withColumn("date", to_date(df.col("timestamp")))
      .write
      .partitionBy("date")
      .partitionBy("Time")
      .format("parquet")
      .mode(SaveMode.Ignore)
      .save("MachineResultInCommon.parquet")





    val parquetFileByDateDF: Unit = spark.read.parquet("MachineResultInCommon.parquet")
      .createOrReplaceTempView("pfile")

    val timeTemperaturequery: Unit = spark.sql("select * from pfile WHERE time BETWEEN '03:30' and '08:30' and date='2010-02-25'").show

  }
}