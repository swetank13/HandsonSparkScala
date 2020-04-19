package com.handsonSparkScala

import java.io._ ////We don’t have a class to write a file, in the Scala standard library, so we borrow java.io._ from Java. Or you could import java.io.File and java.io.PrintWriter.
import scala.io.Source //Now Scala does provide a class to read files. This is the class Source. We use its companion object to read files.

object Multithreading {
  
  def main(args: Array[String]) = {
  
  //Creating a New File in Scala
  val writer = new PrintWriter(new File("C:/Outputdata/demo.txt"))
  
  //Writing to the File in Scala
  writer.write("I am writing a content to demo file \nThis is Swetank \nI am Anand")
  
  //At this point, nothing is really visible in the file. To see these changes reflect in the file demo1.txt, we need to close it with Scala.
  writer.close()
  }
  //To read individual lines instead of the whole file at once, we can use the getLines() method. For this,  we change the contents of our file to this:
  Source.fromFile("C:/Outputdata/demo.txt").getLines.foreach{lines=>println(lines)}
  
}
  