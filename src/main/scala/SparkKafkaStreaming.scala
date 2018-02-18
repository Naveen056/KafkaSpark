
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object SparkKafkaStreaming {
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Kafka Streaming application").
      setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(2))
    val topicset = Set("test")
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "localhost:9092")

    val stream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicset)
    val messages = stream.map(s=>s._2)

    val data = messages
    data.print()
    stream.print()

//    val departmentMessages = messages.
//      filter(msg => {
//        val endPoint = msg.split(" ")(6)
//        endPoint.split("/")(1) == "department"
//      })
//    val departments = departmentMessages.
//      map(rec => {
//        val endPoint = rec.split(" ")(6)
//        (endPoint.split("/")(2), 1)
//      })
//    val departmentTraffic = departments.
//      reduceByKey((total, value) => total + value)
//    departmentTraffic.saveAsTextFiles("/user/dgadiraju/deptwisetraffic/cnt")

    ssc.start()
    ssc.awaitTermination()

  }
}
