package com.atguigu.chapter05

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.Properties


/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object SinkToKafkaTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取文件数据
//    val stream = env.readTextFile("input/clicks.txt")

    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))
      .map(data => {
        val fields = data.split(",")
        Event(fields(0).trim, fields(1).trim, fields(2).trim.toLong).toString
      })

    // 将数据写入到kafka
    stream.addSink( new FlinkKafkaProducer[String]("hadoop102:9092", "events", new SimpleStringSchema()) )

    env.execute()
  }
}
