package com.sundogsoftware.spark


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

//because calculations are so large, this needs to be run on a cluster

object Next_Rev_Scala {
  case class Auth_Table(Time:String, SourceUserDomain:String, DestinationUserDomain:String, SourceComputer:String, DestinationComputer:String, AuthenticationType:String,LogonType:String, AuthenticationOrientation:String, SuccessFailure:String)
  
  def mapper(line:String): Auth_Table = {
    val fields = line.split(',')  
    
    val authorization:Auth_Table = Auth_Table(fields(0), fields(1), fields(2), fields(3),fields(4),fields(5),fields(6),fields(7),fields(8))
    return authorization
  }
  
  case class Red_Table(Time:String, UserDomain:String,SourceComputer:String,DestinationComputer:String)
    def mapper2(line2:String): Red_Table = {
    val fields2 = line2.split(',')  
    
    val red_table:Red_Table = Red_Table(fields2(0), fields2(1), fields2(2), fields2(3))
    return red_table
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("Next_Rev_Scala")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Convert our txt file to a DataSet, using our Auth_Table case
    // class to infer the schema.
    import spark.implicits._
    val lines = spark.sparkContext.textFile("../newsample.csv")
    val computers = lines.map(mapper).toDS().cache()
    
    val lines2 = spark.sparkContext.textFile("../redteam.txt")
    val redteam = lines2.map(mapper2).toDS().cache()
    
    //add column to redteam in order to tell that it is redteam or not
    val redteampos = redteam.withColumn("RedTeam?", lit("True"))
    
    
    println("Here is our inferred Authorization schema:")
    computers.printSchema()
    
    println("Here is our inferred redteam schema:")
    redteampos.printSchema()
    
    println("Group up Authorization's Time")
    computers.groupBy("Time").count().show()
    
    println("Group up red Teams' Time")
    redteampos.groupBy("Time").count().show()
    
    println("Group by Red Team's UserDomain:")
    val UserCount = redteampos.groupBy("UserDomain").count()
    UserCount.show()
    
    println("How many distinct User Domain's are there?")
    UserCount.agg(countDistinct("UserDomain")).show()
    
    println("Group by Success or Failure:")
    computers.groupBy("SuccessFailure").count().show()
    
    println("Group by Source Computer Type:")
    val sourceComp = computers.groupBy("SourceComputer").count()
    sourceComp.filter(sourceComp("count") > 1).show()
    
    println("Group by Source Computer Type for red team:")
    redteampos.groupBy("SourceComputer").count().show()
    
    println("Group by Source and Destination Computer for the whole group excluding if source and destination are the same:")
    val SDComputers = computers.groupBy(computers("SourceComputer"),computers("DestinationComputer")).count()
    SDComputers.filter(SDComputers("SourceComputer") =!= SDComputers("DestinationComputer")).show()
    
    println("Group by Source and Destination Computer for the red team not excluding if source and destination are the same:")
    val redSDComputers = redteampos.groupBy(redteampos("SourceComputer"),redteampos("DestinationComputer")).count()
    redSDComputers.show()
   
    spark.stop()
  }
}