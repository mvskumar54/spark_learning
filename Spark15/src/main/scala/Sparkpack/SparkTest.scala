package Sparkpack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkTest {
  
  def main(args:Array[String]):Unit={
  		val conf=new SparkConf().setAppName("NEW").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark=SparkSession.builder().getOrCreate()
					import spark.implicits._
					
					val src_data_df=spark.read.format("json").option("multiLine", "true").load("file:///C:/Users/SwathiSanthosh/Desktop/hadoop/spark source//batters.json")
src_data_df.show()
src_data_df.printSchema()

  }
  
}