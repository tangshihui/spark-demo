package com.example.spark

import org.apache.spark.sql.SparkSession

object BroadcastTest {

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
    val session = SparkSession.builder().appName("Score").master("local[1]").getOrCreate()

    val scoreLevel = Map[String, Int => Boolean](
      "A" -> {
        _ >= 90
      },
      "B" -> { x => x >= 80 && x < 90 },
      "C" -> { x => x >= 70 && x < 80 },
      "D" -> { x => x < 70 },
    )
    //broadcast
    val broadcastScoreLevel = session.sparkContext.broadcast(scoreLevel)
    //accumulator
    val totalScoreACC = session.sparkContext.longAccumulator("Total score")


    val linesRDD = session.read.textFile("data/score.txt").rdd
    val nameLevelRDD = linesRDD.map(line => {
      val itmes = line.split(" ")
      val (name, score) = (itmes(0), itmes(1).toInt)
      //sum score
      totalScoreACC.add(score)
      //judge level
      val level = broadcastScoreLevel.value.find(kv => kv._2(score)).get._1

      (name, level, score)
    })

    println("--------------score level--------------")
    nameLevelRDD.foreach(println(_))

    println("--------------total score--------------")
    println(totalScoreACC.value)


    session.stop()
  }


}
