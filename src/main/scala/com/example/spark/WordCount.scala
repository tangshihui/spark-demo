package com.example.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  /**
   * spark command:
   * bin/spark-submit --class com.example.spark.WordCount --master yarn --deploy-mode cluster /home/ec2-user/test/spark-demo.jar WordCount hdfs://10.100.39.212:8020/test/LICENSE.txt hdfs://10.100.39.212:8020/test/LICENSE_WC.txt
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(s"args ERROR. Usage: WordCount <input-file> <output-file>\nCurrent args:${args.mkString(" ")}")
      System.exit(1)
    }

    val file       = args(1) // "hdfs://10.100.39.212:8020/test/LICENSE.txt" //args(1)
    val outputFile = args(2) //"hdfs://10.100.39.212:8020/test/LICENSE_WC.txt"//

    //val file = args(1) //"hdfs://10.100.39.212:8020/test/LICENSE.txt" //
    val session = SparkSession.builder().appName("WordCount").getOrCreate()

    val lines = session.read.textFile(file).rdd
    val words = lines.flatMap(_.split(" "))
    val wordC = words.filter(_.nonEmpty).map(w => (w, 1))
    val wc = wordC.reduceByKey(_ + _)

    val sorted = wc.collect().sortBy(_._2).reverse
    sorted.foreach(println(_))

    session.sparkContext.parallelize(sorted).saveAsTextFile(outputFile)

    session.stop()
  }


}
