package com.atguigu.chapter08

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object ConnectTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 定义两条整数流
    val stream1 = env.fromElements(1, 2, 3)
    val stream2 = env.fromElements(1L, 2L, 3L)

    // 连接两条流
    stream1.connect(stream2)
      .map(new CoMapFunction[Int, Long, String] {
        override def map1(value: Int): String = s"Int: $value"

        override def map2(value: Long): String = s"Long: $value"
      })
      .print()

    env.execute()
  }
}
