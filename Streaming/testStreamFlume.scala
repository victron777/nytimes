package bigdata.spark_apps

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.sql.functions._
import java.util.Calendar
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.util._
import org.apache.log4j.{ Level, Logger }
import org.json4s._
import org.json4s.JsonDSL.WithDouble._

object testStreamFlume {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumePollingEventClient <host> <port>")
      System.exit(1)
    }

    val Array(host, port) = args

    val batchInterval = Milliseconds(15000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumePollingEventClient").setMaster("local[6]")
    val sc = new SparkContext(sparkConf) 
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.sql("set spark.sql.shuffle.partitions=1")
    
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val ssc = new StreamingContext(sc, batchInterval)

    // Create a flume stream that polls the Spark Sink running in a Flume agent
    val stream = FlumeUtils.createPollingStream(ssc, host, port.toInt)
 
    val cal = Calendar.getInstance()
    val date = (cal.get(Calendar.YEAR ).toString + "" + cal.get(Calendar.MONTH ).toString + "" + cal.get(Calendar.DATE ).toString)
    val hour = (cal.get(Calendar.HOUR ).toString + "" + cal.get(Calendar.MINUTE ).toString )
    
    val result = stream.map(e => new String(e.event.getBody.array())).
    filter(x => x.split("\"")(2).split(" ")(1) == "200").map(x => (x.split("\"")(1).split(" ")(1).split("/")(1),x.split("\"")(2).split(" ")(2) ))
    
    val result_map = result.map(x=> ((x._1.toString),(x._2.toString.toDouble,1,x._2.toString.toDouble,x._2.toString.toDouble)))
    
    val numOfPartitions = 9 // specify the amount you want
    val hashPartitioner = new HashPartitioner(numOfPartitions)
    var comByKeyResult =
result_map.combineByKey((x:(Double,Int,Double,Double))=>(x._1,x._2,x._3,x._4),
(x:(Double,Int,Double,Double),y:(Double,Int,Double,Double))=>(x._1+y._1,x._2+y._2,math.max(x._3,y._3),math.min(x._4,y._4)),
(x:(Double,Int,Double,Double),y:(Double,Int,Double,Double))=>(x._1+y._1,x._2+y._2,math.max(x._3,y._3),math.min(x._4,y._4)), hashPartitioner).
map(x => (x._1,x._2._1/x._2._2,x._2._3,x._2._4,x._2._1))

comByKeyResult.map(x => ("{\"app\":\"" + x._1 + "\",\"stat\":" + x._2 + "}")).repartition(1).
saveAsTextFiles("hdfs://quickstart.cloudera:8022/user/cloudera/nytimes/avg" + "/" + date + "/" + hour,"json")

comByKeyResult.map(x => ("{\"app\":\"" + x._1 + "\",\"stat\":" + x._3 + "}")).repartition(1).
saveAsTextFiles("hdfs://quickstart.cloudera:8022/user/cloudera/nytimes/max" + "/" + date + "/" + hour,"json")

comByKeyResult.map(x => ("{\"app\":\"" + x._1 + "\",\"stat\":" + x._4 + "}")).repartition(1).
saveAsTextFiles("hdfs://quickstart.cloudera:8022/user/cloudera/nytimes/min" + "/" + date + "/" + hour,"json")

comByKeyResult.map(x => ("{\"app\":\"" + x._1 + "\",\"stat\":" + x._5 + "}")).repartition(1).
saveAsTextFiles("hdfs://quickstart.cloudera:8022/user/cloudera/nytimes/tot" + "/" + date + "/" + hour,"json")

    ssc.start()
ssc.awaitTermination()
  }
}


