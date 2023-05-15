package com.atguigu.chapter05

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object TransformMapTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L)
    )

    // 提取每次点击事件的用户名
    // 1. 使用匿名函数
    stream.map( _.user ).print("1")

    // 2. 实现MapFunction接口
    stream.map(new UserExtractor).print("2")

    env.execute()
  }

  class UserExtractor extends MapFunction[Event, String] {
    override def map(t: Event): String = t.user
  }
}
