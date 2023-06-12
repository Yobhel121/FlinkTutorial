package com.atguigu.chapter09

import com.atguigu.chapter05.{ClickSource, Event}
import org.apache.flink.api.common.functions.{AggregateFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object AverageTimestampExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
    stream
      .keyBy(_.user)
      .flatMap( new AvgTimestamp )
      .print("result")

    stream.print("input")

    env.execute()
  }

  // 实现自定义的RichFlatmapFunction
  class AvgTimestamp extends RichFlatMapFunction[Event, String]{
    // 定义聚合状态
    lazy val avgTsAggState: AggregatingState[Event, Long] = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[Event, (Long, Long), Long](
      "avg-ts",
      new AggregateFunction[Event, (Long, Long), Long] {
        override def createAccumulator(): (Long, Long) = (0L, 0L)

        override def add(in: Event, acc: (Long, Long)): (Long, Long) = (acc._1 + in.timestamp, acc._2 + 1)

        override def getResult(acc: (Long, Long)): Long = acc._1 / acc._2

        override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = ???
      },
      classOf[(Long, Long)]
    ))

    // 定义一个值状态，保存当前已经到达的数据个数
    lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))

    override def flatMap(in: Event, collector: Collector[String]): Unit = {
      avgTsAggState.add(in)
      // 更新count值
      val count = countState.value()
      countState.update(count + 1)

      // 判断是否达到了计数窗口的长度，输出结果
      if (countState.value() == 5){
        collector.collect(s"${in.user}的平均时间戳为：${avgTsAggState.get()}")
        // 窗口销毁
        countState.clear()
      }
    }
  }
}
