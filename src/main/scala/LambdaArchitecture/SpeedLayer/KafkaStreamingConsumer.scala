package LambdaArchitecture.SpeedLayer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, TableName }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.HColumnDescriptor
import scala.io.Source
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.fs.shell.Count
import org.apache.spark.streaming.dstream.ForEachDStream

/**
 * @author hadoop
 */

object KafkaStreamingConsumer {
  def main(args: Array[String]): Unit = {

    val brokers = "HPC-Server:9092"
    val topics = "page_visits"

    //create ssc context with 60 secs batch interval
    val conf = new SparkConf().setAppName("Kafka Word Count").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(20))

    //create direct kafka stream with brokers and topics

    val topicset = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "zookeeper.connect" -> "HPC-Server:2182")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicset)
    //Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    lines.foreachRDD { rdd =>
      {
        rdd.foreach(record => {
          var file = scala.xml.XML.loadString(record.toString())
          val ips = (file \\ "ip").map { ele => ele.text }
          val sites = (file \\ "site").map { ele => ele.text }
          val secs = (file \\ "secs").map { ele => ele.text }
          var i = 0
          var n = ips.length - 1
          var ht = new HBaseInsert
          for (i <- 0 to n) {
            ht.InsertRecord(secs(i), ips(i), sites(i))
          }
        })
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
class HBaseInsert {
  def InsertRecord(runtime: String, ip: String, site: String) {
    val conf = HBaseConfiguration.create()
    val htable = new HTable(conf, "page_visits")
    val p = new Put(Bytes.toBytes(runtime))
    p.add(Bytes.toBytes("Address"), Bytes.toBytes("IP"), Bytes.toBytes(ip))
    p.add(Bytes.toBytes("Address"), Bytes.toBytes("site"), Bytes.toBytes(site))
    htable.put(p)
    println("Row Inserted")
    htable.close()
  }
}