package com.atguigu.chapter08

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

object SplitStreamTest {

  // 定义输出标签
  val maryTag = OutputTag[(String, String, Long)]("mary-tag")
  val bobTag = OutputTag[(String, String, Long)]("bob-tag")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)

    // 按照不同用户的点击事件，进行分流操作
    val mary_stream = stream.filter(_.user == "Mary")
    val bob_stream = stream.filter(_.user == "Bob")
    val else_stream = stream.filter(data => data.user != "Mary" && data.user != "Bob")

    //    mary_stream.print("mary")
    //    bob_stream.print("bob")
    //    else_stream.print("else")

    // 使用侧输出流进行分流操作
    val elseStream = stream.process(new ProcessFunction[Event, Event] {
      override def processElement(value: Event, ctx: ProcessFunction[Event, Event]#Context, out: Collector[Event]): Unit = {
        if (value.user == "Mary") {
          ctx.output(maryTag, (value.user, value.url, value.timestamp))
        } else if (value.user == "Bob") {
          ctx.output(bobTag, (value.user, value.url, value.timestamp))
        } else {
          out.collect(value)
        }
      }
    })

    elseStream.print("else")
    elseStream.getSideOutput(maryTag).print("mary")
    elseStream.getSideOutput(bobTag).print("bob")

    env.execute()
  }
}
