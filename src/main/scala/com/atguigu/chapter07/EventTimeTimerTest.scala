package com.atguigu.chapter07

import com.atguigu.chapter05.{ClickSource, Event}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object EventTimeTimerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new CustomSource)
      .assignAscendingTimestamps(_.timestamp)

    stream.keyBy(data => true)
      .process( new KeyedProcessFunction[Boolean, Event, String] {
        override def processElement(value: Event, ctx: KeyedProcessFunction[Boolean, Event, String]#Context, out: Collector[String]): Unit = {
          val currentTime = ctx.timerService().currentWatermark()
          out.collect(s"数据到达，当前时间是：$currentTime, 当前数据时间戳是：${value.timestamp}")
          // 注册一个5秒之后的定时器
          ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5 * 1000)
        }

        // 定义定时器触发时的执行逻辑
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Boolean, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect("定时器触发，触发时间为：" + timestamp)
        }
      } )
      .print()

    env.execute()
  }

  class CustomSource extends SourceFunction[Event]{
    override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
      // 直接发出测试数据
      ctx.collect(Event("Mary", "./home", 1000L))

      // 间隔5秒钟
      Thread.sleep(5000)

      // 继续发出数据
      ctx.collect(Event("Mary", "./home", 2000L))
      Thread.sleep(5000)

      ctx.collect(Event("Mary", "./home", 6000L))
      Thread.sleep(5000)

      ctx.collect(Event("Mary", "./home", 6001L))
      Thread.sleep(5000)
    }

    override def cancel(): Unit = ???
  }
}
