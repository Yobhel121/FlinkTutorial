package com.atguigu.chapter08

import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.lang

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object CoGroupTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1 = env.fromElements(
      ("a", 1000L),
      ("b", 1000L),
      ("a", 2000L),
      ("b", 6000L)
    ).assignAscendingTimestamps(_._2)

    val stream2 = env.fromElements(
      ("a", 3000L),
      ("b", 3000L),
      ("a", 4000L),
      ("b", 4000L)
    ).assignAscendingTimestamps(_._2)

    // 窗口同组联结操作
    stream1.coGroup(stream2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply( new CoGroupFunction[(String, Long), (String, Long), String] {
        override def coGroup(iterable: lang.Iterable[(String, Long)], iterable1: lang.Iterable[(String, Long)], collector: Collector[String]): Unit = {
          collector.collect(iterable + " => " + iterable1)
        }
      })
      .print()

    env.execute()
  }
}
