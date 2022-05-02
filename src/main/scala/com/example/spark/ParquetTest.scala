package com.example.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetTest {


  /**
   * parquet压缩：不大不小
   * parquet读取：不快不慢
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("Sql").master("local[*]").getOrCreate()

    val userDF = session.read.json("data/user.json")

    println("--------------write to parquet--------------")
    userDF.write.mode(SaveMode.Overwrite).parquet("data/user_parquet")

    println("--------------Read from parquet--------------")
    val userPq = session.read.parquet("data/user_parquet")
    userPq.show()

    session.stop()
  }


}
