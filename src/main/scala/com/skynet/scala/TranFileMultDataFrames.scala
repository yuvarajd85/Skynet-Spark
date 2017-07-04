package com.skynet.scala
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SQLImplicits;

object TranFileMultDataFrames {
  def main(args: Array[String]){
		if(args.length < 1){
			System.err.println("No input arguments passed");
			System.exit(99);
		}

		val conf = new SparkConf().setAppName("TranDataFrames").setMaster("local[*]");
		val sc = new SparkContext(conf);
		val sqlContext = new SQLContext(sc);
		import sqlContext.implicits._;
		val textFileOct = sc.textFile(args(0));
		val textFileNov = sc.textFile(args(1));
		val dfOct = textFileOct.map ( x => x.split(",") ).map ( x => (x(0).trim, x(1).toDouble, x(2).trim, x(3).trim, x(4).toDouble) ).toDF;
		val dfNov = textFileOct.map ( x => x.split(",") ).map ( x => (x(0).trim, x(1).toDouble, x(2).trim, x(3).trim, x(4).toDouble) ).toDF;
		val dfTot = dfOct.unionAll(dfNov);
		dfTot.registerTempTable("tran");
		val tranRecSum = sqlContext.sql("select _3, sum(_5) as sum from tran group by _3 order by _3, sum desc");
		tranRecSum.rdd.coalesce(1, true).saveAsTextFile(args(2));

	}
}