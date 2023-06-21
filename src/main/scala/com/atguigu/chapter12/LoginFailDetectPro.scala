package com.atguigu.chapter12

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object LoginFailDetectPro {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 读取数据源
    val loginEventStream = env.fromElements(
      LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
      LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
      LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
      LoginEvent("user_2", "192.168.1.29", "success", 6000L)
    ).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
    .withTimestampAssigner(new SerializableTimestampAssigner[LoginEvent] {
      override def extractTimestamp(t: LoginEvent, l: Long): Long = t.timestamp
    }))

    // 2. 定义Pattern，检测连续三次登录失败事件
    val pattern = Pattern.begin[LoginEvent]("fail").where( _.eventType == "fail" ).times(3).consecutive()
      .within(Time.seconds(10))

    // 3. 将模式应用到事件流上，检测匹配的复杂事件
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), pattern)

    // 4. 定义处理规则，将检测到的匹配事件报警输出
    val resultStream: DataStream[String] = patternStream.process(new PatternProcessFunction[LoginEvent, String] {
      override def processMatch(map: util.Map[String, util.List[LoginEvent]], context: PatternProcessFunction.Context, collector: Collector[String]): Unit = {
        // 获取匹配到的复杂事件
        val firstFail = map.get("fail").get(0)
        val secondFail = map.get("fail").get(1)
        val thirdFail = map.get("fail").get(2)

        // 包装报警信息输出
        collector.collect(s"${firstFail.userId} 连续三次登录失败！登录时间：${firstFail.timestamp}, ${secondFail.timestamp}, ${thirdFail.timestamp}")
      }
    })

    resultStream.print()

    env.execute()
  }
}
