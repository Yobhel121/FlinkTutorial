package com.atguigu.chapter05

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object TransformRichFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Bob", "./cart", 3000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=1", 4000L),
      Event("Mary", "./prod?id=3", 6000L),
      Event("Mary", "./prod?id=2", 5000L)
    )

    // 自定义一个RichMapFunction，测试富函数类的功能
    stream.map( new MyRichMap() )
      .print()

    env.execute()
  }

  class MyRichMap() extends RichMapFunction[Event, Long]{

    override def open(parameters: Configuration): Unit = {
      println("索引号为" + getRuntimeContext.getIndexOfThisSubtask + "的任务开始")
    }

    override def map(in: Event): Long = in.timestamp

    override def close(): Unit = {
      println("索引号为" + getRuntimeContext.getIndexOfThisSubtask + "的任务结束")
    }
  }
}
