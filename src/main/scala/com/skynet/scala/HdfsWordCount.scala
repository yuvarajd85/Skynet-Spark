package com.skynet.scala
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.StreamingContext; 
import org.apache.spark.streaming.Seconds;
import org.apache.spark.SparkContext;

object HdfsWordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }

//    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[*]")
    // Create the context
//    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val sc = new SparkContext(sparkConf)

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    /*val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()*/
    
    val textFile = sc.textFile(args(0));
    val count = textFile.flatMap { lines => lines.split(",") }.map {word=>(word,1) }.reduceByKey(_+_);
    count.saveAsTextFile(args(1));
    sc.stop();
  }
}