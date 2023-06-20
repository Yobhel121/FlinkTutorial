package com.atguigu.chapter11

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object UdfTest_ScalarFunction {
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

    // 2. 注册标量函数
    tableEnv.createTemporarySystemFunction("myHash", classOf[MyHash])

    // 3. 调用函数进行查询转换
    val resultTable = tableEnv.sqlQuery("select uid, myHash(uid) from eventTable")

    // 4. 结果打印输出
    tableEnv.toDataStream(resultTable).print()

    env.execute()
  }

  // 实现自定义的标量函数，哈希函数
  class MyHash extends ScalarFunction {
    def eval(str: String): Int = {
      str.hashCode
    }
  }

}
