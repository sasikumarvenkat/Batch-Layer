package LambdaArchitecture.BatchLayer

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, TableName }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
import org.apache.hadoop.hbase.HColumnDescriptor
import scala.io.Source
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.fs.shell.Count

object BatchXML {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)    
    var file = scala.xml.XML.loadFile("books.xml")
    val bookids = (file \\ "book").map { ele => (ele \ "@id").text }
    val authors = (file \\ "author").map { ele => ele.text }
    val titles = (file \\ "title").map { ele => ele.text }
    val genres = (file \\ "genre").map { ele => ele.text }
    val publish_dates = (file \\ "publish_date").map { ele => ele.text }
    val prices = (file \\ "price").map { ele => ele.text }
    var i = 0
    var n = bookids.length - 1
    var ht = new HBaseInsert
    for (i <- 0 to n) 
    {       
      
      ht.InsertRecord(bookids(i), authors(i), titles(i), genres(i), publish_dates(i), prices(i))
    }
  }

}
class HBaseInsert {
  def InsertRecord(bookid: String, author: String, title: String, genre: String, pd: String, price: String) {
    val conf = HBaseConfiguration.create()
    val htable = new HTable(conf, "books")
    val p = new Put(Bytes.toBytes(bookid))
    p.add(Bytes.toBytes("author_det"), Bytes.toBytes("aname"), Bytes.toBytes(author))
    p.add(Bytes.toBytes("book_det"), Bytes.toBytes("title"), Bytes.toBytes(title))
    p.add(Bytes.toBytes("book_det"), Bytes.toBytes("genre"), Bytes.toBytes(genre))
    p.add(Bytes.toBytes("book_det"), Bytes.toBytes("price"), Bytes.toBytes(price))
    p.add(Bytes.toBytes("author_det"), Bytes.toBytes("publish_date"), Bytes.toBytes(pd))
    htable.put(p)
    println("Row Inserted")
    htable.close()
  }
}