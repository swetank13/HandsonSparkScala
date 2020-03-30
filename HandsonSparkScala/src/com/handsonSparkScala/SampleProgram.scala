package com.handsonSparkScala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object SampleProgram {
  def main(args: Array[String]) {
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().setAppName("Sneh Exercise New 05")
    .setMaster(appConf.getConfig(args(2)).getString("deployment"))
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val outputPath = args(1)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPathExists = fs.exists(new Path(inputPath))
    val outputPathExists = fs.exists(new Path(outputPath))

    if (!inputPathExists) {
      println("Input Path does not exists")
      return
    }

    if (outputPathExists) {
      fs.delete(new Path(outputPath), true)
    }

    val departments = sc.textFile(inputPath + "/departments")
    val categories = sc.textFile(inputPath + "/categories")
    val products = sc.textFile(inputPath + "/products")
    val orders = sc.textFile(inputPath + "/orders")
    val orderItems = sc.textFile(inputPath + "/order_items")

    val departmentsMap = departments.map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))
    val categoriesMap = categories.map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))
    val productsMap = products.map(rec => (rec.split(",")(1).toInt, rec.split(",")(0)))

    val prodCat = productsMap.join(categoriesMap)
    val prodCatMap = prodCat.map(rec => (rec._2._2.toInt, rec._2._1))
    val prodDept = prodCatMap.join(departmentsMap).distinct

    val prodDeptMap = prodDept.map(rec => (rec._2._1.toInt, rec._2._2))

    val oiMap = orderItems.map(rec => (rec.split(",")(1).toInt, (rec.split(",")(2), rec.split(",")(4))))

    val ordMap = orders.filter(rec => {
      rec.split(",")(3) == "COMPLETE"
    }).map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))

    val oiO = oiMap.join(ordMap)

    val oiOMap = oiO.map(rec => (rec._2._1._1.toInt, (rec._2._2, rec._2._1._2)))

    val ordProd = oiOMap.join(prodDeptMap)
    
    val ordProdMap = ordProd.map(rec=>((rec._2._1._1,rec._2._2),rec._2._1._2.toFloat)).reduceByKey(_+_).sortByKey()
    
    ordProdMap.saveAsTextFile(outputPath)
    
    sc.stop()
  }        
    }