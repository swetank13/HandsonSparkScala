package com.handsonSparkScala

object ScalaProgram extends App{  
    
  val setList1 = Set("Swet","Swetank","Anand","And")
  val setList2 = Set("Big Data", "Scala", "Hive", "Sqoop")
  
  println(s"The print the list $setList1")
  for(finalList1 <- setList1)
  {
    println(finalList1)
  }
  
}