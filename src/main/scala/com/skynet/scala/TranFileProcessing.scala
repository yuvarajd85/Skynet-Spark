package com.skynet.scala

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object TranFileProcessing {
	def main(args: Array[String]){
		if(args.length < 1){
			System.err.println("No input arguments passed");
			System.exit(99);
		}

		val conf  = new SparkConf().setAppName("TxnProc").setMaster("local[*]");

		val sc = new SparkContext(conf);

		val tranFile = sc.textFile(args(0));

		val txnFilter = tranFile.filter(line => line.contains(args(2).toString()));
		val txnFm = txnFilter.map(line => line.split(",")).map(txn => (txn(2), txn(4).toDouble));
		val txnFmRed = txnFm.reduceByKey(_+_);
		val outputCollect = txnFmRed.coalesce(1,true);
		outputCollect.saveAsTextFile(args(1));

	}
}