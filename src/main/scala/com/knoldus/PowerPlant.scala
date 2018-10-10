package com.knoldus

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object PowerPlant {
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

    val df = spark.read.format("com.databricks.spark.csv")
            .schema(customSchema)
            .load("data.csv")

    val partitionByDate: Unit = df.withColumn("date",to_date(df.col("TimeStamp")))
                      .write
                      .partitionBy("date")
                      .format("parquet")
                      .mode(SaveMode.Ignore )
                      .save("MachineResult.parquet")


    def getTime(timeStamp:String)={
      val newtime=timeStamp.slice(11,16)
              newtime }

    val f = udf(getTime _, StringType)

    val partitionByTime: Unit =df.withColumn("Time",f(df.col("TimeStamp")))
                          .write
                          .partitionBy("time")
                          .format("parquet")
                          .mode(SaveMode.Ignore )
                          .save("MachineResultTime.parquet")



    val parquetFileByDateDF: Unit = spark.read.parquet("MachineResult.parquet")
                        .createOrReplaceTempView("pfile")

    val datetemperaturequeryDf: Unit = spark.sql("select Temperature from pfile WHERE date BETWEEN '2010-02-25' and '2010-02-26'").show

    val parquetFileByTimeDF: Unit = spark.read.parquet("MachineResultTime.parquet")
      .createOrReplaceTempView("pfile1")
    val timeTemperaturequery: Unit = spark.sql("select * from pfile1 WHERE time BETWEEN '03:30' and '08:30'").show

 }

}
