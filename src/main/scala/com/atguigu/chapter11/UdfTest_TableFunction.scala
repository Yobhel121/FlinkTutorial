package com.atguigu.chapter11

import com.atguigu.chapter11.UdfTest_ScalarFunction.MyHash
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object UdfTest_TableFunction {
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

    // 2. 注册表函数
    tableEnv.createTemporarySystemFunction("mySplit", classOf[MySplit])

    // 3. 调用函数进行查询转换
    val resultTable = tableEnv.sqlQuery("SELECT uid, url, word, len " +
      "FROM eventTable, LATERAL TABLE(mySplit(url)) AS T(word, len)")

    // 4. 结果打印输出
    tableEnv.toDataStream(resultTable).print()

    env.execute()
  }

  // 实现自定义的表函数，按照？分割url字段
  @FunctionHint(output = new DataTypeHint("ROW<word STRING, len INT>"))
  class MySplit extends TableFunction[Row]{
    def eval(str: String): Unit = {
      str.split("\\?").foreach( s => collect(Row.of(s, Int.box(s.length))))
    }
  }
}
