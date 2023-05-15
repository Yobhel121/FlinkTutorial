package com.atguigu.chapter05

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object TransformReduceTest {
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

    // reduce归约聚合，提取当前最活跃用户
    stream.map( data => (data.user, 1L) )
      .keyBy(_._1)
      .reduce( new MySum() )    // 统计每个用户的活跃度
      .keyBy(data => true)    // 将所有数据按照同样的key分到同一个组中
      .reduce( (state, data) => if (data._2 >= state._2) data else state)    // 选取当前最活跃的用户
      .print()

    env.execute()
  }

  class MySum() extends ReduceFunction[(String, Long)]{
    override def reduce(t: (String, Long), t1: (String, Long)): (String, Long) = (t._1, t._2 + t1._2)
  }
}
