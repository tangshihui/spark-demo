package com.example.spark

import org.apache.spark.sql.SparkSession

object BroadcastTest {

  /**
   *
   * broadcast variable
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
