package com.example.spark

import org.apache.spark.sql.SparkSession

object SqlTest {

  /**
   *
   * broadcast variable
   *  - 普通变量在每一个task上都会copy一份，网络IO开销大
   *  - broadcast变量只需在executor上copy一份，executor上的tasks共享这份数据，减少不必要的网络IO
   *
   * accumulator
   *  - 普通变量的累加只会在task上执行累加结果，在driver端获取不到累加结果
   *  - accumulator变量可以将task累加的结果发送到driver端，driver端可以汇总所有task的累加结果
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("Sql").master("local[*]").getOrCreate()

    val userDF = session.read.json("data/user.json")

    println("--------------原生API--------------")
    println(userDF.schema)
    userDF.show()
    userDF
      .select(userDF.col("name"))
      .where(userDF("age").gt(20))
      .show()

    println("--------------Spark SQL--------------")
    userDF.createTempView("user")
    session
      .sql("select * from user where age > 22")
      .show()


    session.stop()
  }


}
