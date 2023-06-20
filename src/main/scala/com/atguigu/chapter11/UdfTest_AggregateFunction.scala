package com.atguigu.chapter11

import com.atguigu.chapter11.UdfTest_TableFunction.MySplit
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.AggregateFunction

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object UdfTest_AggregateFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 1. 创建表
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

    // 2. 注册聚合函数
    tableEnv.createTemporarySystemFunction("weightedAvg", classOf[WeightedAvg])

    // 3. 调用函数进行查询转换
    val resultTable = tableEnv.sqlQuery("SELECT uid, weightedAvg(ts, 1) as avg_ts FROM eventTable GROUP BY uid ")

    // 4. 结果打印输出
    tableEnv.toChangelogStream(resultTable).print()

    env.execute()
  }

  // 单独定义样例类，用来表示聚合过程中累加器的类型
  case class WeightedAvgAccumulator(var sum: Long = 0, var count: Int = 0)

  // 实现自定义的聚合函数，计算加权平均数
  class WeightedAvg extends AggregateFunction[java.lang.Long, WeightedAvgAccumulator]{
    override def getValue(acc: WeightedAvgAccumulator): java.lang.Long = {
      if (acc.count == 0){
        null
      } else {
        acc.sum / acc.count
      }
    }

    override def createAccumulator(): WeightedAvgAccumulator = WeightedAvgAccumulator()   // 创建累加器

    // 每来一条数据，都会调用
    def accumulate(acc: WeightedAvgAccumulator, iValue: java.lang.Long, iWeight: Int): Unit ={
      acc.sum += iValue
      acc.count += iWeight
    }
  }
}
