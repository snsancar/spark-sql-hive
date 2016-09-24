package com.sankar

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

object HiveInsertUsingSpark extends App {

  case class Payment(PaymentDate: String, PaymentNumber: String, VendorName: String, Category: String, Amount: String)

  val conf = new SparkConf().setAppName("Spark Dataframe").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  import sqlContext.implicits._

  val inputFile = sc.textFile("filepath")
  val columns = inputFile.map(_.split("\\|")).filter { cols => cols(1) != "null" }

  val hiveSchemaRDD = columns.map(p => Payment(p(0), p(1), p(2).toUpperCase(), p(3), "[$,]".r.replaceAllIn(p(4), "")))
  val hiveDataFrame = hiveSchemaRDD.toDF()
  hiveDataFrame.registerTempTable("paymenttemptable")
  val transformedDF = sqlContext.sql("select PaymentDate, PaymentNumber, VendorName, Category, sum(Amount) as amt from paymenttemptable group by PaymentDate, PaymentNumber, VendorName, Category")
  transformedDF.write.mode(SaveMode.Append).saveAsTable("payment")
}