package com.handsonSparkScala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object Assessment {
  
    case class DataSetSchema(ts: String, number: Int, pick_lat: Float, pick_lng: Float, drop_lat: Float, drop_lng: Float)
  def parseLine(lines: String): DataSetSchema = {
    val fields = lines.split(",")
    val ts = fields(0)
    val number = fields(1).toInt
    val pick_lat = fields(2).toFloat
    val pick_lng = fields(3).toFloat
    val drop_lat = fields(4).toFloat
    val drop_lng = fields(5).toFloat
    DataSetSchema(ts, number,pick_lat,pick_lng, drop_lat, drop_lng)
  } 
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("Assessment")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      import spark.implicits._
    
      //Loading the data from local file system in case of any other file system we need to provide the URL and then file path 
      val data = spark.sparkContext.textFile("/C:/data/RapidoAssignment/ct_rr_withoutheader.csv")
      
      //Applying the schema on data and converting to dataset
      val requiredDF = data.map(parseLine).toDS()
      
      //requiredDF.printSchema()      
      //val limitedRecord = requiredDF.show(10) //Checking the data after printing 20 records
     
      requiredDF.createOrReplaceTempView("data_table")
      //val tableList = spark.sql("show tables").show()   
     
      val main_table = spark.sql("Select CONCTNS.number,CONCTNS.datetime[0] As datefield,CONCTNS.datetime[1] As timefield From (Select number, split(ts,' ') As datetime from data_table)CONCTNS")
      //main_table.show()
      
      main_table.createOrReplaceTempView("main_table")
      //val tableList = spark.sql("show tables").show()
      
      val noOfCustomerPerDay = spark.sql("Select substring(datefield,1,2) As perDay, count(number) from main_table group by datefield")
      noOfCustomerPerDay.show()
      
    /*  val results = checkQuery.collect()
    
      results.foreach(println)
*/
      spark.stop()
    
       //val splittedColumn = mappedData.map(line => (line.split(",")(0).substring(0,10).replace("-", "").toInt, 1))
          
 }
}