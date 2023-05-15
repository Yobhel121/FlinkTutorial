package com.atguigu.chapter02

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)
    env.disableOperatorChaining()

    // 2. 读取socket文本流数据
    //    val lineDataStream = env.socketTextStream("hadoop102", 7777)
    val parameterTool = ParameterTool.fromArgs(args)
    val hostname = parameterTool.get("host")
    val port = parameterTool.getInt("port")
    val lineDataStream = env.socketTextStream(hostname, port)

    // 3. 对数据集进行转换处理
    val wordAndOne = lineDataStream.flatMap(_.split(" ")).slotSharingGroup("1").map(word => (word, 1))

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
