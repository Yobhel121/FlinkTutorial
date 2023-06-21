package com.atguigu.chapter12

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._

import java.util

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

// 定义登录事件的样例类
case class LoginEvent(userId: String, ipAddr: String, eventType: String, timestamp: Long)

object LoginFailDetect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 读取数据源
    val loginEventStream = env.fromElements(
      LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
      LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
      LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
      LoginEvent("user_2", "192.168.1.29", "success", 6000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
    ).assignAscendingTimestamps(_.timestamp)

    // 2. 定义Pattern，检测连续三次登录失败事件
    val pattern = Pattern.begin[LoginEvent]("firstFail").where( _.eventType == "fail" )  // 第一次登录失败事件
      .next("secondFail").where(_.eventType == "fail")    // 第二次登录失败事件
      .next("thirdFail").where(_.eventType == "fail")    // 第三次登录失败事件

    // 3. 将模式应用到事件流上，检测匹配的复杂事件
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), pattern)

    // 4. 定义处理规则，将检测到的匹配事件报警输出
    val resultStream: DataStream[String] = patternStream.select(new PatternSelectFunction[LoginEvent, String] {
      override def select(map: util.Map[String, util.List[LoginEvent]]): String = {
        // 获取匹配到的复杂事件
        val firstFail = map.get("firstFail").get(0)
        val secondFail = map.get("secondFail").get(0)
        val thirdFail = map.get("thirdFail").get(0)

        // 包装报警信息输出
        s"${firstFail.userId} 连续三次登录失败！登录时间：${firstFail.timestamp}, ${secondFail.timestamp}, ${thirdFail.timestamp}"
      }
    })

    resultStream.print()

    env.execute()
  }
}
