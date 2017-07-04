package com.skynet.scala

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;

object NamesGrouping {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("NamesGrouping").setMaster("local[*]");
    val sc = new SparkContext(conf);
    val out = sc.textFile("G://Alienware-Files//BOFA//Input//CountyNames.txt").map { name => (name.charAt(0), name) }.groupByKey().mapValues { names => names.toSet.size }.sortByKey(true, 1).coalesce(1, true).saveAsTextFile("G://Alienware-Files//BOFA//Input//CountyNamesGroupNew") ;
    val totLinesCount = sc.textFile("G://Alienware-Files//BOFA//Input//CountyNames.txt", 1).count();
    println("Total number of Lines: %s".format(totLinesCount));
  }
}