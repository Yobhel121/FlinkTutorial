package com.atguigu.chapter05

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object PartitionCustomTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取自定义的数据源
    val stream = env.fromElements(1,2,3,4,5,6,7,8)

    // 自定义重分区策略
    stream.partitionCustom(new Partitioner[Int] {
      override def partition(k: Int, i: Int): Int = {
        k % 2
      }
    }, data => data)
      .print("custom").setParallelism(4)

    env.execute()
  }
}
