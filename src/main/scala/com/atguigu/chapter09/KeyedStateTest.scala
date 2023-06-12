package com.atguigu.chapter09

import com.atguigu.chapter05.{ClickSource, Event}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.storage.{FileSystemCheckpointStorage, JobManagerCheckpointStorage}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object KeyedStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(10000)

    // 检查点的存储位置
    env.getCheckpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage(""))

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .flatMap( new MyFlatMap )
      .print()

    env.execute()
  }

  // 自定义一个RichMapFunction
  class MyFlatMap extends RichFlatMapFunction[Event, String] {
    // 定义状态
    var valueState: ValueState[Event] = _
    var listState: ListState[Event] = _
    var mapState: MapState[String, Long] = _

    var reducingState: ReducingState[Event] = _
    var aggState: AggregatingState[Event, String] = _

    // 定义一个本地属性
    var localCount = 0

    override def open(parameters: Configuration): Unit = {
      // 配置Ttl
      val config = StateTtlConfig.newBuilder(Time.seconds(10))
        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build()

      val valueDescriptor = new ValueStateDescriptor[Event]("my-value", classOf[Event])
      valueDescriptor.enableTimeToLive(config)

      valueState = getRuntimeContext.getState(valueDescriptor)
      listState = getRuntimeContext.getListState(new ListStateDescriptor[Event]("my-list", classOf[Event]))
      mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("my-map", classOf[String], classOf[Long]))

      reducingState = getRuntimeContext.getReducingState(new ReducingStateDescriptor[Event]("my-reduce",
        new ReduceFunction[Event] {
          override def reduce(t: Event, t1: Event): Event = Event(t.user, t.url, t1.timestamp)
        }, classOf[Event]
      ))

      aggState = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[Event, Long, String]("my-agg",
        new AggregateFunction[Event, Long, String] {
          override def createAccumulator(): Long = 0L

          override def add(in: Event, acc: Long): Long = acc + 1

          override def getResult(acc: Long): String = "聚合状态为：" + acc.toString

          override def merge(acc: Long, acc1: Long): Long = ???
        }, classOf[Long]
      ))
    }

    override def flatMap(in: Event, collector: Collector[String]): Unit = {
      // 对状态进行操作
      println("值状态为：" + valueState.value())
      valueState.update(in)
      println("值状态为：" + valueState.value())

      listState.add(in)

      println("--------------")

      val count = if(mapState.contains(in.user)) mapState.get(in.user) else 0
      mapState.put(in.user, count + 1)
      println(s"用户 ${in.user} 的访问频次为： ${mapState.get(in.user)}")

      println("--------------")
      reducingState.add(in)
      println(reducingState.get())

      println("--------------")
      aggState.add(in)
      println(aggState.get())

      localCount += 1
      println(localCount)

      println("==============================")
    }
  }
}
