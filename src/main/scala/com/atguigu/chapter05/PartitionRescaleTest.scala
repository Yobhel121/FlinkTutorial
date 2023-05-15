package com.atguigu.chapter05

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object PartitionRescaleTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取自定义的数据源
    val stream = env.addSource(new RichParallelSourceFunction[Int] {
      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        for (i <- 0 to 7){
          // 利用运行时上下文中的subTask的id信息，来控制数据由哪个并行子任务生成
          if ( getRuntimeContext.getIndexOfThisSubtask == (i + 1) % 2 )
            ctx.collect( i + 1 )
        }
      }

      override def cancel(): Unit = ???
    }).setParallelism(2)

    // 轮询重分区后打印输出
    stream.rescale.print("rescale").setParallelism(4)

    env.execute()
  }
}
