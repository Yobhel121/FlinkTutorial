package com.atguigu.chapter05

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

import java.util.Calendar
import scala.util.Random

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

class ClickSource extends SourceFunction[Event]{
  // 标志位
  var running = true

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    // 随机数生成器
    val random = new Random()
    // 定义数据随机选择的范围
    val users = Array("Mary", "Alice", "Bob", "Cary")
    val urls = Array("./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2", "./prod?id=3")

    // 用标志位作为循环判断条件，不停地发出数据
    while (running){
      val event = Event(users(random.nextInt(users.length)), urls(random.nextInt(urls.length)), Calendar.getInstance.getTimeInMillis)
//      // 为要发送的数据分配时间戳
//      ctx.collectWithTimestamp(event, event.timestamp)
//
//      // 向下游直接发送水位线
//      ctx.emitWatermark(new Watermark(event.timestamp - 1L))

      // 调用ctx的方法向下游发送数据
      ctx.collect(event)

      // 每隔1s发送一条数据
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = running = false
}
