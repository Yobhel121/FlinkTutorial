package com.atguigu.chapter11

import com.atguigu.chapter05.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object TopNWindowExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    val eventStream = env.fromElements(
      Event("Alice", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
      Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
      Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
      Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
      Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
    ).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
      }))

    // 将DataStream转换成表
    val eventTable = tableEnv.fromDataStream(eventStream, $("url"), $("user").as("uid"),
      $("timestamp").as("ts"), $("et").rowtime())

    tableEnv.createTemporaryView("eventTable", eventTable)

    // 窗口TOP N，选取每小时内活跃度最大的前两个用户
    // 1. 进行窗口聚合统计，计算每个用户目前的访问量
    val urlCountWindowTable = tableEnv.sqlQuery(
      """
        |SELECT uid, COUNT(url) AS cnt, window_start, window_end
        |FROM TABLE (
        |  TUMBLE(TABLE eventTable, DESCRIPTOR(et), INTERVAL '1' HOUR)
        |)
        |GROUP BY uid, window_start, window_end
        |""".stripMargin)
    tableEnv.createTemporaryView("urlCountWindowTable", urlCountWindowTable)
    // 2. 提取count值最大的前两个用户
    val top2ResultTable = tableEnv.sqlQuery(
      """
        |SELECT *
        |FROM (
        |  SELECT *, ROW_NUMBER() OVER (
        |   PARTITION BY window_start, window_end
        |   ORDER BY cnt DESC
        |  ) as row_num
        |  FROM urlCountWindowTable
        |)
        |WHERE row_num <= 2
        |""".stripMargin)

    tableEnv.toDataStream(top2ResultTable).print()

    env.execute()
  }
}
