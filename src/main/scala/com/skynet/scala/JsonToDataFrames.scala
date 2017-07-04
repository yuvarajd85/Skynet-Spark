package com.skynet.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object JsonToDataFrames {
  val conf = new SparkConf().setAppName("JsonToDataFrames").setMaster("local[*]");
  val sc = new SparkContext(conf);
  val sqlContext = new org.apache.spark.sql.SQLContext(sc);
  
  def main(args: Array[String]){
    
    import sqlContext.implicits._;
    
    val itemData = sqlContext.read.json("G://Alienware-Files//BOFA//Input//itemjson.txt");
    itemData.registerTempTable("items");
    
    val result = sqlContext.sql("select * from items where name like 'm%'");
    
    //result.show();
    result.rdd.coalesce(1).saveAsTextFile("G://Alienware-Files//BOFA//Input//itemJsonOut");
    
  }
  
}