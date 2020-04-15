package com.skynet.scala

import org.apache.spark.sql.SparkSession;

object SparkHiveLocal {
  
  val spark : SparkSession = SparkSession.builder().appName("SparkHiveLocal").config("mapreduce.input.fileinputformat.input.dir.recursive", true).config("spark.sql.warehouse.dir", "C://Users//yuvaraj//git//Skynet-Spark//src//est//scala//META-INF//").config("hive.metastore.warehouse.dir", "C://Users//yuvaraj//git//Skynet-Spark//src//est//scala//META-INF//").enableHiveSupport().master("local[*]").getOrCreate();
  
  def main(args: Array[String]){
//    val file = spark.read.csv("G://Alienware-Files//BOFA//Input//TranIn//January2016_6032.csv");
    val file1 = spark.read.format("com.databricks.spark.csv").option("inferSchema", true).option("header", true).load("G://Alienware-Files//BOFA//Input//TranIn//January2016_6032.csv");
    file1.printSchema();
    file1.write.saveAsTable("tranJan");
    println(System.getProperty("user.dir"));
    
  }
}