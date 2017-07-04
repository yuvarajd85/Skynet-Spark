package com.skynet.scala

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SQLImplicits;

object TranFileDataFrames {

	def main(args: Array[String]){
		if(args.length < 1){
			System.err.println("No input arguments passed");
			System.exit(99);
		}

		val conf = new SparkConf().setAppName("TranDataFrames").setMaster("local[*]");
		val sc = new SparkContext(conf);
		val sqlContext = new SQLContext(sc);
		import sqlContext.implicits._;
		val textFile = sc.textFile(args(0));
		val df = textFile.map ( x => x.split(",") ).map ( x => (x(0).trim, x(1).toDouble, x(2).trim, x(3).trim, x(4).toDouble) ).toDF;
		df.registerTempTable("tran");
		val tranRecSum = sqlContext.sql("select _3, sum(_5) from tran where _3 like 'WAWA%' group by _3");
		tranRecSum.rdd.coalesce(1, true).saveAsTextFile(args(1));
//		sqlContext.read.json("path");
	}
}