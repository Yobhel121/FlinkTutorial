package com.atguigu.chapter11

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object CommonApiTest {
  def main(args: Array[String]): Unit = {
    // 1. 创建表环境
    // 1.1 直接基于流执行环境创建
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 1.2 传入一个环境的配置参数创建
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tableEnvironment = TableEnvironment.create(settings)

    // 2. 创建表
    tableEnv.executeSql("CREATE TABLE eventTable (" +
      " uid STRING," +
      " url STRING," +
      " ts BIGINT" +
      ") WITH (" +
      " 'connector' = 'filesystem'," +
      " 'path' = 'input/clicks.txt'," +
      " 'format' = 'csv'" +
      ") ")

    // 3. 表的查询转换
    // 3.1 SQL
    val resultTable = tableEnv.sqlQuery("select uid, url, ts from eventTable where uid = 'Alice'")
    // 统计每个用户访问频次
    val urlCountTable = tableEnv.sqlQuery("select uid, count(url) from eventTable group by uid")
    // 创建虚拟表
    tableEnv.createTemporaryView("tempTable", resultTable)

    // 3.2 Table API
    val eventTable = tableEnv.from("eventTable")
    val resultTable2 = eventTable.where($("url").isEqual("./home"))
      .select($("url"), $("uid"), $("ts"))

    // 4. 输出表的创建
    tableEnv.executeSql("CREATE TABLE outputTable (" +
      " user_name STRING," +
      " url STRING," +
      " `timestamp` BIGINT" +
      ") WITH (" +
      " 'connector' = 'filesystem'," +
      " 'path' = 'output'," +
      " 'format' = 'csv'" +
      ") ")

    // 5. 将结果表写入输出表中
//    resultTable.executeInsert("outputTable")

    // 6. 转换成流打印输出
    tableEnv.toDataStream(resultTable).print("result")
    tableEnv.toChangelogStream(urlCountTable).print("count")

    env.execute()
  }
}