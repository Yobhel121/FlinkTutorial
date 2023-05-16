package com.atguigu.chapter06

import com.atguigu.chapter05.{ClickSource, Event}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object FullWindowFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    // 测试全窗口函数，统计uv
    stream.keyBy( data => "key" )
      .window( TumblingEventTimeWindows.of(Time.seconds(10)) )
      .process(new UvCountByWindow)
      .print()

    env.execute()
  }

  // 自定义实现ProcessWindowFunction
  class UvCountByWindow extends ProcessWindowFunction[Event, String, String, TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
      // 使用一个Set进行去重操作
      var userSet = Set[String]()

      // 从elements中提取所有数据，依次放入set中去重
      elements.foreach( userSet += _.user )
      val uv = userSet.size
      // 提取窗口信息包装String进行输出
      val windowEnd = context.window.getEnd
      val windowStart = context.window.getStart

      out.collect(s"窗口 $windowStart ~ $windowEnd 的uv值为：$uv")
    }
  }
}
