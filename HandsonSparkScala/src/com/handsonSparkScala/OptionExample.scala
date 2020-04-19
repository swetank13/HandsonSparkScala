package com.handsonSparkScala

object OptionExample extends App{
  
    //Getting the value using get method
    val glazedDonutTaste: Option[String] = Some("Very Tasty")
    println(s"The glazed donut taste is ${glazedDonutTaste.get}")
    
    //Getting the value using getOrElse() method
    val glazedDonutName: Option[String] = Some("Glazed Donut")
    println(s"The name of glazed donut is ${glazedDonutName.getOrElse("Glazed Donut")}")
    
    //Getting the value using pattern matching
    glazedDonutName match {
      case Some(name) => println(s"The name of donut is $name")
      case None => println("The donut name is not found")
    }
    
    //Define an Option in a function parameter
    def calculateTotalCost(name: String, quantity: Int, couponCode: Option[String]): Double = {
      couponCode match {
        case Some(coupon) =>
            val discount = 0.1
            val totalCost = 2.5 * quantity * (1 - discount)
            totalCost
        case None => 2.5 * quantity
      }      
    }
    println(s"""The total cost is ${calculateTotalCost("Glazed Donut", 10, Some("CouponCode"))}""")
    
    
  
}