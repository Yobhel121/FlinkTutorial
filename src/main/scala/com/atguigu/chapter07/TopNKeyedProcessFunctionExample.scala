package com.atguigu.chapter07

import com.atguigu.chapter05.ClickSource
import com.atguigu.chapter06.UrlViewCount
import com.atguigu.chapter06.UrlViewCountExample.{UrlViewCountAgg, UrlViewCountResult}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object TopNKeyedProcessFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    // 1. 结合使用增量聚合函数和全窗口函数，统计每个url的访问频次
    val urlCountStream = stream.keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .aggregate(new UrlViewCountAgg, new UrlViewCountResult)

    // 2. 按照窗口信息进行分组提取，排序输出
    val resultStream = urlCountStream.keyBy(_.windowEnd)
      .process(new TopN(2))

    resultStream.print()

    env.execute()
  }

  // 实现自定义KeyedProcessFunction
  class TopN(n: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{
    // 声明列表状态
    var urlViewCountListState: ListState[UrlViewCount] = _

    override def open(parameters: Configuration): Unit = {
      urlViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("list-state", classOf[UrlViewCount]))
    }

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      // 每来一个数据，就直接放入ListState中
      urlViewCountListState.add(value)
      // 注册一个窗口结束时间1ms之后的定时器
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 先把数据提取出来放到List里
      val urlViewCountList = urlViewCountListState.get().toList
      val topnList = urlViewCountList.sortBy(-_.count).take(n)

      // 结果包装输出
      val result = new StringBuilder()
      result.append(s"============窗口：${timestamp - 1 - 10000} ~ ${timestamp - 1}===========\n")
      for (i <- topnList.indices){
        val urlViewCount = topnList(i)
        result.append(s"浏览量Top ${i+1} ")
          .append(s"url: ${urlViewCount.url} ")
          .append(s"浏览量是： ${urlViewCount.count} \n")
      }

      out.collect(result.toString())
    }
  }
}
