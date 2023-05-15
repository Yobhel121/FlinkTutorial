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

object TransformUDFTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Bob", "./cart", 3000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=1", 4000L),
      Event("Mary", "./prod?id=3", 6000L),
      Event("Mary", "./prod?id=2", 5000L)
    )

    // 测试UDF的用法，筛选url中包含某个关键字home的Event事件
    // 1. 实现一个自定义的函数类
    stream.filter( new MyFilterFunction("prod") )
      .print("1")

    // 2. 使用匿名类
    stream.filter( new FilterFunction[Event] {
      override def filter(t: Event): Boolean =  t.url.contains("prod")
    } )
      .print("2")

    // 3. 使用lambda表达式
    stream.filter( _.url.contains("prod") )
      .print("3")

    env.execute()
  }

  // 实现自定义的FilterFunction
  class MyFilterFunction(keyWord: String) extends FilterFunction[Event]{
    override def filter(t: Event): Boolean = t.url.contains(keyWord)
  }
}
