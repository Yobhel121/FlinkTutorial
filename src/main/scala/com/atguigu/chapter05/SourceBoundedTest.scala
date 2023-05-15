package com.atguigu.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

case class Event(user: String, url: String, timestamp: Long)

object SourceBoundedTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 从元素中读取数据
    val stream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    val stream1: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L)
    )

    // 2. 从集合中读取数据
    val clicks = List(Event("Mary", "./home", 1000L), Event("Bob", "./cart", 2000L))
    val stream2: DataStream[Event] = env.fromCollection(clicks)

    // 3. 从文件中读取数据
    val stream3: DataStream[String] = env.readTextFile("input/clicks.txt")

    // 打印输出
    stream.print("number")
    stream1.print("1")
    stream2.print("2")
    stream3.print("3")

    env.execute()
  }
}
