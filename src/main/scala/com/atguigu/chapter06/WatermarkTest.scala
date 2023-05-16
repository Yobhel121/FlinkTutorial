package com.atguigu.chapter06

import com.atguigu.chapter05.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(500L)

    val stream: DataStream[Event] = env.socketTextStream("hadoop102", 7777)
      .map( data => {
        val fields = data.split(",")
        Event(fields(0).trim, fields(1).trim, fields(2).trim.toLong)
      } )

    // 1. 有序流的水位线生成策略
    stream.assignTimestampsAndWatermarks( WatermarkStrategy.forMonotonousTimestamps[Event]()
    .withTimestampAssigner(
      new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
      }
    ))

    // 2. 乱序流的水位线生成策略
    stream.assignTimestampsAndWatermarks( WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(5))
    .withTimestampAssigner(
      new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
      }
    ))
      .keyBy(_.user)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process( new WatermarkWindowResult )
      .print()

    // 3. 自定义周期性水位线的生成
    stream.assignTimestampsAndWatermarks( new WatermarkStrategy[Event] {
      override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Event] = {
        new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
        }
      }

      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Event] = {
        new WatermarkGenerator[Event] {
          // 定义一个延迟时间
          val delay = 5000L
          // 定义属性保存最大时间戳
          var maxTs = Long.MinValue + delay + 1

          override def onEvent(t: Event, l: Long, watermarkOutput: WatermarkOutput): Unit = {
            maxTs = math.max(maxTs, t.timestamp)
          }

          override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
            val watermark = new Watermark(maxTs - delay - 1)
            watermarkOutput.emitWatermark(watermark)
          }
        }
      }
    } )

    env.execute()
  }

  // 实现自定义的全窗口函数
  class WatermarkWindowResult extends ProcessWindowFunction[Event, String, String, TimeWindow]{
    override def process(user: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
      // 提取信息
      val start = context.window.getStart
      val end = context.window.getEnd
      val count = elements.size

      // 增加水位线信息
      val currentWatermark = context.currentWatermark

      out.collect(s"窗口 $start ~ $end , 用户 $user 的活跃度为：$count, 水位线现在位于：$currentWatermark")
    }
  }
}
