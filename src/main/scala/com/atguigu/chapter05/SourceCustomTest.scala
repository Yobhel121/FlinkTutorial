package com.atguigu.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object SourceCustomTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取自定义的数据源
    val stream: DataStream[Event] = env.addSource(new ClickSource).setParallelism(2)

    stream.print()

    env.execute()
  }
}
