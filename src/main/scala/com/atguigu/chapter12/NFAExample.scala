package com.atguigu.chapter12

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object NFAExample {
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

    val resultStream = loginEventStream.keyBy(_.userId)
      .flatMap( new StateMachineMapper() )

    resultStream.print()

    env.execute()
  }

  // 实现自定义的RichFlatmapFunction
  class  StateMachineMapper() extends RichFlatMapFunction[LoginEvent, String]{
    // 定义一个状态机的状态
    lazy val currentState = getRuntimeContext.getState(new ValueStateDescriptor[State]("state", classOf[State]))

    override def flatMap(in: LoginEvent, collector: Collector[String]): Unit = {
      if (currentState.value() == null){
        currentState.update(Initial)
      }

      val nextState = transition(currentState.value(), in.eventType)

      nextState match {
        case Matched => collector.collect(s"${in.userId}连续三次登录失败")
        case Terminal => currentState.update(Initial)
        case _ => currentState.update(nextState)
      }
    }
  }

  // 将状态State定义为封闭的特征
  sealed  trait State
  case object Initial extends State
  case object Terminal extends State
  case object Matched extends State
  case object S1 extends State
  case object S2 extends State

  // 定义状态转移函数
  def transition(state: State, eventType: String): State = {
    (state, eventType) match {
      case (Initial, "success") => Terminal
      case (Initial, "fail") => S1
      case (S1, "success") => Terminal
      case (S1, "fail")  => S2
      case (S2, "success") => Terminal
      case (S2, "fail")  => Matched
    }
  }
}
