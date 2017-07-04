package com.skynet.scala

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

object SparkCSV {
  val conf = new SparkConf().setAppName("SparkCSV").setMaster("local[*]");
  val sc = new SparkContext(conf);
  val ssc = new SQLContext(sc);
  import ssc.implicits._;
  
  def main(args: Array[String]){
    val dataIn = ssc.read.format("com.databricks.spark.csv")
                 .option("header", true)
                 .option("inferSchema", true)
                 .load("G://Alienware-Files//BOFA//Input//TranIn//January2016_6032.csv");
    
    dataIn.filter($"Payee".startsWith("WAWA")).agg($"Amount");
    
  }
}