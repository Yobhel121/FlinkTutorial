package com.atguigu.chapter07

import com.atguigu.chapter05.{ClickSource, Event}
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

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    stream.process(new ProcessFunction[Event, String]{
      override def processElement(value: Event, ctx: ProcessFunction[Event, String]#Context, out: Collector[String]): Unit = {
        if (value.user.equals("Mary"))
          out.collect(value.user)
        else if (value.user.equals("Bob")){
          out.collect(value.user)
          out.collect(value.url)
        }
        println(getRuntimeContext.getIndexOfThisSubtask)
        println(ctx.timerService().currentWatermark())
        //        ctx.timerService().registerEventTimeTimer()
      }

      //      override def onTimer(timestamp: Long, ctx: ProcessFunction[Event, String]#OnTimerContext, out: Collector[String]): Unit = {
      //        super.onTimer(timestamp, ctx, out)
      //      }
    })
      .print()

    env.execute()
  }
}
