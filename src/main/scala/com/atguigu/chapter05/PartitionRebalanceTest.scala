package com.atguigu.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object PartitionRebalanceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取自定义的数据源
    val stream = env.addSource(new ClickSource)

    // 轮询重分区后打印输出
    stream.rebalance.print("rebalance").setParallelism(4)

    env.execute()
  }
}
