package com.atguigu.chapter08

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object BillCheckExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 来自app的支付日志流，（order-id, status, timestamp）
    val appStream = env.fromElements(
      ("order-1", "success", 1000L),
      ("order-2", "success", 2000L)
    )
      .assignAscendingTimestamps(_._3)

    // 2. 来自第三方支付平台的日志流, (order-id, status, platform-id, timestamp)
    val thirdpartyStream = env.fromElements(
      ("order-1", "success", "wechat", 3000L),
      ("order-3", "success", "alipay", 4000L)
    )
      .assignAscendingTimestamps(_._4)

    // 连接两条流进行匹配数据检测
    appStream.connect(thirdpartyStream)
      .keyBy(_._1, _._1)
      .process(new CoProcessFunction[(String, String, Long), (String, String, String, Long), String] {
        // 定义状态变量，用来保存已经到达的事件
        var appEvent: ValueState[(String, String, Long)] = _
        var thirdpartyEvent: ValueState[(String, String, String, Long)] = _

        override def open(parameters: Configuration): Unit = {
          appEvent = getRuntimeContext.getState(new ValueStateDescriptor[(String, String, Long)]("app-event", classOf[(String, String, Long)]))
          thirdpartyEvent = getRuntimeContext.getState(new ValueStateDescriptor[(String, String, String, Long)]("thirdparty-event", classOf[(String, String, String, Long)]))
        }

        override def processElement1(value: (String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#Context, out: Collector[String]): Unit = {
          if (thirdpartyEvent.value() != null){
            out.collect(s"${value._1} 对账成功")
            // 清空状态
            thirdpartyEvent.clear()
          } else {
            // 如果另一条流中的事件没有到达，就注册定时器，开始等待
            ctx.timerService().registerEventTimeTimer(value._3 + 5000)
            // 保存当前事件到状态中
            appEvent.update(value)
          }
        }

        override def processElement2(value: (String, String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#Context, out: Collector[String]): Unit = {
          if (appEvent.value() != null){
            out.collect(s"${value._1} 对账成功")
            // 清空状态
            appEvent.clear()
          } else {
            // 如果另一条流中的事件没有到达，就注册定时器，开始等待
            ctx.timerService().registerEventTimeTimer(value._4 + 5000)
            // 保存当前事件到状态中
            thirdpartyEvent.update(value)
          }
        }

        override def onTimer(timestamp: Long, ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
          // 判断状态是否为空，如果不为空，说明另一条流中对应的事件没来
          if (appEvent.value() != null){
            out.collect(s"${appEvent.value()._1} 对账失败，第三方平台支付事件未到")
          }

          if (thirdpartyEvent.value() != null){
            out.collect(s"${thirdpartyEvent.value()._1} 对账失败，app支付事件未到")
          }

          appEvent.clear()
          thirdpartyEvent.clear()
        }
      })
      .print()

    env.execute()
  }
}
