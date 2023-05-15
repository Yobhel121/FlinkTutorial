package com.atguigu.chapter05

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object TransformFilterTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L)
    )

    // 过滤出用户为Mary的所有点击事件

    // 1. 使用匿名函数
    stream.filter( _.user == "Mary" ).print("1")

    // 2. 实现FilterFunction接口
    stream.filter( new UserFilter ).print("2")

    env.execute()
  }

  class UserFilter extends FilterFunction[Event]{
    override def filter(t: Event): Boolean = t.user == "Bob"
  }
}
