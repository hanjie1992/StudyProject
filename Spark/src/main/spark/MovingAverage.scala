import java.text.SimpleDateFormat

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * scala中的移动平均值通过repartitionandsortWithInPartitions（）进行“二次排序
  */
object MovingAverage {

  def main(args: Array[String]): Unit = {
//    if (args.size < 4){
//      println("参数错误")
//      sys.exit(1)
//    }

    val sparkConf: SparkConf = new SparkConf().setAppName("MovingAverage").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)

//    val window = args(0).toInt
//    val numPartitions = args(1).toInt
//    val input = args(2)
//    val output = args(3)

    val window = 3
    val numPartitions = 2
    val input = "G:\\data\\spark\\SortByMRF_MovingAverage.txt"
    val output = "G:\\data\\spark\\SortByMRF_MovingAverage"

    val path: Path = new Path(output)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(output))){
      fs.delete(new Path(output), true)
    }

    val brodcastWindow: Broadcast[Int] = sc.broadcast(window)

    val rawData: RDD[String] = sc.textFile(input)

    val valueTokey: RDD[(CompositeKey, TimeSeriesData)] = rawData.map(line => {
      val tokens: Array[String] = line.split(",")
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val timestamp: Long = dateFormat.parse(tokens(1)).getTime
      (CompositeKey(tokens(0),timestamp),TimeSeriesData(timestamp,tokens(2).toDouble))
    })
    valueTokey.foreach(x => println("valueTokey= "+x._1,x._2))

    val sortedData: RDD[(CompositeKey, TimeSeriesData)] = valueTokey.repartitionAndSortWithinPartitions(new CompositeKeyPartitioner(numPartitions))
    sortedData.foreach(x => println("sortedData= "+x._1,x._2))

    val keyValue: RDD[(String, TimeSeriesData)] = sortedData.map(k => (k._1.stockSymbol,(k._2)))
    keyValue.foreach(x => println("keyValue= "+x._1,x._2))

    val groupByStockSymbol: RDD[(String, Iterable[TimeSeriesData])] = keyValue.groupByKey()
    groupByStockSymbol.foreach(f => println("groupByStockSymbol= "+f))

    val movingAverage: RDD[(String, Iterable[(String, Double)])] = groupByStockSymbol.mapValues(values => {
      val dateFormat: SimpleDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val queue = new scala.collection.mutable.Queue[Double]()
      for (TimeSeriesData <- values) yield {
        queue.enqueue(TimeSeriesData.closingStockPrice)
        if (queue.size > brodcastWindow.value){
          queue.dequeue()
        }
        (dateFormat.format(new java.util.Date(TimeSeriesData.timeStamp)),(queue.sum / queue.size))
      }
    })
    movingAverage.foreach(f => println("movingAverage= "+f))

    val formattedResult: RDD[String] = movingAverage.flatMap(kv => {
      kv._2.map(v => (kv._1+","+v._1+","+v._2.toString))
    })
    formattedResult.foreach(f => println("formattedResult= "+f))
    formattedResult.saveAsTextFile(output)
    sc.stop()
  }
}

case class CompositeKey(stockSymbol :String ,timeStamp : Long)
case class TimeSeriesData(timeStamp: Long , closingStockPrice: Double)
//定义排序
object CompositeKey{
  implicit def ordering[A <: CompositeKey]:Ordering[A] = {
    Ordering.by(fk => (fk.stockSymbol,fk.timeStamp))
  }
}

/**
  * 下面的类通过扩展抽象类org.apache.spark.partitioner来定义自定义分区器
  */
import org.apache.spark.Partitioner
class CompositeKeyPartitioner(partitions: Int) extends Partitioner{
  require(partitions >= 0,s"分区数($partitions)不能为负数")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case k: CompositeKey => math.abs(k.stockSymbol.hashCode % numPartitions)
    case null => 0
    case _ => math.abs(key.hashCode() % numPartitions)
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case h: CompositeKeyPartitioner => h.numPartitions == numPartitions
    case _ => false
  }
  override def hashCode(): Int = numPartitions
}