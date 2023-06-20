package com.atguigu.chapter11

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object TopNExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 创建表
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

    // TOP N，选取活跃度最大的前两个用户
    // 1. 进行分组聚合统计，计算每个用户目前的访问量
    val urlCountTable = tableEnv.sqlQuery("select uid, count(url) as cnt from eventTable group by uid")
    tableEnv.createTemporaryView("urlCountTable", urlCountTable)
    // 2. 提取count值最大的前两个用户
    val top2ResultTable = tableEnv.sqlQuery(
      """
        |SELECT uid, cnt, row_num
        |FROM (
        |  SELECT *, ROW_NUMBER() OVER (
        |   ORDER BY cnt DESC
        |  ) as row_num
        |  FROM urlCountTable
        |)
        |WHERE row_num <= 2
        |""".stripMargin)

    tableEnv.toChangelogStream(top2ResultTable).print()

    env.execute()

  }
}
