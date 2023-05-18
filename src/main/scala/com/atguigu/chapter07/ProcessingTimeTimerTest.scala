package com.atguigu.chapter07

import com.atguigu.chapter05.{ClickSource, Event}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object ProcessingTimeTimerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    stream.keyBy(data => true)
      .process( new KeyedProcessFunction[Boolean, Event, String] {
        override def processElement(value: Event, ctx: KeyedProcessFunction[Boolean, Event, String]#Context, out: Collector[String]): Unit = {
          val currentTime = ctx.timerService().currentProcessingTime()
          out.collect("数据到达，当前时间是：" + currentTime)
          // 注册一个5秒之后的定时器
          ctx.timerService().registerProcessingTimeTimer(currentTime + 5 * 1000)
        }

        // 定义定时器触发时的执行逻辑
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Boolean, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect("定时器触发，触发时间为：" + timestamp)
        }
      } )
      .print()

    env.execute()
  }
}
