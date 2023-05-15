package com.atguigu.chapter05

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object TransformAggTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=1", 4000L),
      Event("Mary", "./prod?id=3", 6000L),
      Event("Mary", "./prod?id=2", 5000L)
    )

    //    stream.keyBy( _.user )
    //    stream.keyBy( _.url )
    stream.keyBy( new MyKeySelector() )
      .maxBy("timestamp")
      .print()

    env.execute()

  }

  class MyKeySelector() extends KeySelector[Event, String] {
    override def getKey(in: Event): String = in.user
  }
}
