package com.skynet.scala

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object LocationFlatMap {
  
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("LocationFilterRDD").setMaster("local[*]");
    val sc = new SparkContext(conf);
    
    sc.textFile("G://Alienware-Files//BOFA//Input//EmployeeDet.txt", 1).flatMap { x => (x + "RajestOut") }.coalesce(1, true).saveAsTextFile("G://Alienware-Files//BOFA//Input//EmployeeLocFlatMap");
    /*sc.textFile("G://Alienware-Files//BOFA//Input//EmployeeDet.txt", 1).map(n => n.split(",")).map { x => (
            for( i <- 0 until x.size){
              println("input strings: " + x(i))
              (x(i), 1)
              val wordMap = Map(x(i) -> 1);
            })}.saveAsTextFile("G://Alienware-Files//BOFA//Input//EmployeeLocMapWordCount");
    */
  }
}