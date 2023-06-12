package com.atguigu.chapter09

import com.atguigu.chapter05.{ClickSource, Event}
import org.apache.flink.api.common.state.MapStateDescriptor
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

object FakeWindowExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.url)
      .process( new FakeWindow(10000L) )    // 模拟一个10秒的滚动窗口
      .print()

    env.execute()
  }

  // 实现自定义的KeyedProcessFunction
  class FakeWindow(size: Long) extends KeyedProcessFunction[String, Event, String]{
    // 定义一个映射状态，用来保存每个窗口的pv值
    lazy val windowPvMapState = getRuntimeContext.getMapState(new MapStateDescriptor[Long, Long]("window-pv", classOf[Long], classOf[Long]))

    override def processElement(value: Event, ctx: KeyedProcessFunction[String, Event, String]#Context, out: Collector[String]): Unit = {
      // 计算当前数据落入的窗口起始时间戳
      val start = value.timestamp / size * size
      val end = start + size

      // 注册一个定时器，用来触发窗口计算
      ctx.timerService().registerEventTimeTimer(end - 1)

      // 更新状态，count加1
      if (windowPvMapState.contains(start)){
        val pv = windowPvMapState.get(start)
        windowPvMapState.put(start, pv + 1)
      } else {
        windowPvMapState.put(start, 1L)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 定时器触发时，窗口输出结果
      val start = timestamp + 1 - size

      val pv = windowPvMapState.get(start)

      // 窗口输出结果
      out.collect(s"url: ${ctx.getCurrentKey} 浏览量为： ${pv}  窗口为：${start} ~ ${start + size}")

      // 窗口销毁
      windowPvMapState.remove(start)
    }
  }
}
