package com.kafka.stream

import org.apache.spark.sql.SparkSession
import org.scalatest.path

object KafkaStructStream {

  def main(args: Array[String]): Unit = {

    println(" -- Started Structured Streaming App --")

    if (args.length < 3) {
      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics>")
      System.exit(1)
    }

    val Array(bootstrapServers, subscribeType, topics) = args

    val spark = SparkSession
      .builder
      .appName("StructuredKafkaWordCount")
      .getOrCreate()

    println(" -- SparkSession Created Successfully --")

    if (args.length < 3) {
      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics>")
      System.exit(1)
    }


    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, topics)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]


    // Generate running word count
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
 /*   val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start() */

    //Writing in Append Mode, Append mode is not supported on Aggregate
 /*   val query = lines.writeStream
      .outputMode("append")
      .format("console")
      .start()
*/
   //Saving Kafka Output to
    val query = lines.writeStream
   .format("csv")
   .option("checkpointLocation","hdfs://localhost:9000/checkpoint")
   .option("path","hdfs://localhost:9000/sparkOutput")
   .start()

    query.awaitTermination()

  }

}
