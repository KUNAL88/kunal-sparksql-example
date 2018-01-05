package com.kafka.stream

import org.apache.spark.sql.SparkSession

object KafkaStreamingApi {

  def main(args: Array[String]): Unit = {
    println("Welcome to Kafka Streaming API ")

    val appName="Spark-Kakfa-Stream API"
    val bootstrapServers="127.0.0.1:9092"
    val topic="test"
    val startOffset=1
    val endOffset=30

    val sparkSession=SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport().getOrCreate()

    println("--- Spark Session Done! ---")
    val kafkaStreamDF=sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",bootstrapServers)
      .option("subscribe",topic)
      .option("startingOffsets","earliest").load()
      //.option("endingOffsets",endOffset)

    println("--- Kafka Config Done! ---")
    kafkaStreamDF.selectExpr("CAST(key as STRING)","CAST(offset as STRING)","CAST(value as STRING)")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()




    println(" -- Reading Kafka Msg --")
    //null
    //df.explain()
  }

}
