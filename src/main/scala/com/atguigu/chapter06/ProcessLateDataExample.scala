package com.atguigu.chapter06

import com.atguigu.chapter05.{ClickSource, Event}
import com.atguigu.chapter06.UrlViewCountExample.{UrlViewCountAgg, UrlViewCountResult}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

/**
 * Mary, ./home, 1000
 * Mary, ./home, 2000
 * Mary, ./home, 10000
 * Mary, ./home, 15000
 * Mary, ./home, 1000
 * Mary, ./home, 5000
 * Mary, ./home, 8000
 * Mary, ./home, 12000
 * Mary, ./home, 70000
 * Mary, ./home, 1000
 * Mary, ./home, 75000
 */
object ProcessLateDataExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.socketTextStream("hadoop102", 7777)
      .map(data => {
        val fields = data.split(",")
        Event(fields(0).trim, fields(1).trim, fields(2).trim.toLong)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(5))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[Event] {
            override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
          }
        ))

    // 定义一个侧输出流的输出标签
    val outputTag = OutputTag[Event]("late-data")

    // 结合使用增量聚合函数和全窗口函数，包装统计信息
    val result = stream.keyBy(_.url)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      // 指定窗口允许等待的时间
      .allowedLateness(Time.minutes(1))
      // 将迟到数据输出到侧输出流
      .sideOutputLateData(outputTag)
      .aggregate(new UrlViewCountAgg, new UrlViewCountResult)

    result.print("result")

    stream.print("input")

    // 将侧输出流的数据进行打印输出
    result.getSideOutput(outputTag).print("late data")

    env.execute()
  }
}
