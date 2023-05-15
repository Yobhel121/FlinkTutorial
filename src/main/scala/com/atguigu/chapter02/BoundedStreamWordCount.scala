package com.atguigu.chapter02

import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object BoundedStreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 读取文本文件数据
    val lineDataStream = env.readTextFile("input/words.txt")

    // 3. 对数据集进行转换处理
    val wordAndOne = lineDataStream.flatMap(_.split(" ")).map(word => (word, 1))

    // 4. 按照单词进行分组
    val wordAndOneGroup = wordAndOne.keyBy(_._1)

    // 5. 对分组数据进行sum聚合统计
    val sum = wordAndOneGroup.sum(1)

    // 6. 打印输出
    sum.print()

    // 执行任务
    env.execute()
  }
}
