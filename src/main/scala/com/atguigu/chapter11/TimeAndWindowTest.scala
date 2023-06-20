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

object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 1. 在创建表的DDL中指定时间属性字段
    tableEnv.executeSql("CREATE TABLE eventTable (" +
      " uid STRING," +
      " url STRING," +
      " ts BIGINT," +
      " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts/1000) )," +
      " WATERMARK FOR et AS et - INTERVAL '2' SECOND" +
      ") WITH (" +
      " 'connector' = 'filesystem'," +
      " 'path' = 'input/clicks.txt'," +
      " 'format' = 'csv'" +
      ") ")

    // 2. 在将流转换成表的时候指定时间属性字段
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

//    val eventTable = tableEnv.fromDataStream(eventStream, $("url"), $("user").as("uid"),
//  $("timestamp").rowtime().as("ts"))

    tableEnv.from("eventTable").printSchema()
    eventTable.printSchema()

    // 测试累积窗口
    tableEnv.createTemporaryView("eventTable", eventTable)
    val resultTable = tableEnv.sqlQuery(
      """
        |SELECT
        | uid, window_end AS endT, COUNT(url) AS cnt
        |FROM TABLE(
        |  CUMULATE(
        |    TABLE eventTable,
        |    DESCRIPTOR(et),
        |    INTERVAL '30' MINUTE,
        |    INTERVAL '1' HOUR
        |  )
        |)
        |GROUP BY uid, window_start, window_end
        |""".stripMargin)

    // 转换成流打印输出
//    tableEnv.toDataStream(resultTable).print()

    // 测试开窗聚合
    val overResultTable = tableEnv.sqlQuery(
      """
        |SELECT uid, url, ts, AVG(ts) OVER (
        |  PARTITION BY uid
        |  ORDER BY et
        |  ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        |) AS avg_ts
        |FROM eventTable
        |""".stripMargin)

    tableEnv.toDataStream(overResultTable).print("over")

    env.execute()
  }
}
