package com.atguigu.chapter08

import com.atguigu.chapter05.Event
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object UnionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取两条流进行合并
    val stream1 = env.socketTextStream("hadoop102", 7777)
      .map(data => {
        val fields = data.split(",")
        Event(fields(0).trim, fields(1).trim, fields(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp)

    val stream2 = env.socketTextStream("hadoop102", 8888)
      .map(data => {
        val fields = data.split(",")
        Event(fields(0).trim, fields(1).trim, fields(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp)

    stream1.union(stream2)
      .process(new ProcessFunction[Event, String] {
        override def processElement(value: Event, ctx: ProcessFunction[Event, String]#Context, out: Collector[String]): Unit = {
          out.collect(s"当前水位线：${ctx.timerService().currentWatermark()}")
        }
      })
      .print()

    env.execute()
  }
}
