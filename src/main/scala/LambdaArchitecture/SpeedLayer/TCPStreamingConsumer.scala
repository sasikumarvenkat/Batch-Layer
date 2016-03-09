package LambdaArchitecture.SpeedLayer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds

import java.util.Date
/**
 * @author hadoop
 */
object TCPStreamingConsumer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingTCPClient").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val ssc = new StreamingContext(sc, Seconds(1))

    val messages = ssc.socketTextStream("slave1", 1234)    
      
    val words = messages.flatMap(_.split(" ")).map(wr => (wr, 1)).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.saveAsTextFile("hdfs://HPC-Server:9000/TCPStream")
      rdd.foreach(rec => {
        println(rec)

      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}