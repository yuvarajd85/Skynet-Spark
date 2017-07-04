package com.skynet.scala

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object LocationFilterRDD {
  
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("LocationFilterRDD").setMaster("local[*]");
    val sc = new SparkContext(conf);
    
    sc.textFile("G://Alienware-Files//BOFA//Input//EmployeeDet.txt", 1).flatMap(y => y.split(",")).map(x => (x, 1)).reduceByKey(_+_).coalesce(1, true).saveAsTextFile("G://Alienware-Files//BOFA//Input//EmployeeLocCount6");
    
  }
}