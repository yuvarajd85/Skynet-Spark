package com.skynet.scala
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.StreamingContext; 
import org.apache.spark.streaming.Seconds;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object HdfsWordCount {
  def main(args: Array[String]) {
   val log =  LoggerFactory.getLogger("HdfsWordCount");
   
   
    /*if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }*/

//    StreamingExamples.setStreamingLogLevels()
    val spark = SparkSession.builder.appName("Hdfs").master("local[*]").getOrCreate();
    
//    val sparkConf = new SparkConf().set("spark.files.overwrite","true").setAppName("HdfsWordCount").setMaster("local[*]")
    // Create the context
//    val ssc = new StreamingContext(sparkConf, Seconds(2))
//    val sc = new SparkContext(sparkConf)

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    /*val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()*/
  
    val textFile = spark.sparkContext.textFile("G://Alienware-Files//BOFA//Input//TxnIn.txt");
    val count = textFile.flatMap { lines => lines.split(",") }.map {word=>(word,1) }.reduceByKey(_+_);
//    count.coalesce(1).saveAsTextFile("G://Alienware-Files//BOFA//Input//txnCount");
    
   val trn = spark.read.format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load("G:\\Alienware-Files\\BOFA\\Input\\TranIn\\April2016_6032.csv");
   trn.describe("Amount").show();
//   trn.show();
    spark.stop();
  }
}