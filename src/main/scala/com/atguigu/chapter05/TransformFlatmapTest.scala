package com.atguigu.chapter05

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object TransformFlatmapTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Alice", "./cart", 3000L)
    )

    // 测试灵活的输出形式
    stream.flatMap(new MyFlatMap).print()

    env.execute()
  }

  // 自定义实现FlatMapFunction
  class MyFlatMap extends FlatMapFunction[Event, String]{
    override def flatMap(t: Event, collector: Collector[String]): Unit = {
      // 如果当前数据是Mary的点击事件，那么就直接输出user
      if (t.user == "Mary"){
        collector.collect(t.user)
      }
      // 如果当前数据是Bob的点击事件，那么就输出user和url
      else if (t.user == "Bob"){
        collector.collect(t.user)
        collector.collect(t.url)
      }
    }
  }
}
