import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap

/**
  * 求topN，k不唯一的情况
  */
object TopNNonUnique {
  def main(args: Array[String]): Unit = {
//    if (args.length <= 1){
//      println("输入格式不对")
//      sys.exit(1)
//    }

    val sparkConf: SparkConf = new SparkConf().setAppName("TopNNonUnique").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)

    val N: Broadcast[Int] = sc.broadcast(2)
//    val path = args(0)
    val icIn: String = "G:\\data\\spark\\catTopNNonUnique.txt"
    val input: RDD[String] = sc.textFile(icIn)

    val kv: RDD[(String, Int)] = input.map(line => {
      val tokens: Array[String] = line.split(",")
      (tokens(0),tokens(1).toInt)
    })

    val uniqueKeys: RDD[(String, Int)] = kv.reduceByKey(_+_)
    import Ordering.Implicits._
    val partitions: RDD[(Int, String)] = uniqueKeys.mapPartitions(itr => {
      var sortedMap: SortedMap[Int, String] = SortedMap.empty[Int,String]
      itr.foreach{tuple =>
        {
          sortedMap += tuple.swap
          if (sortedMap.size > N.value){
            sortedMap = sortedMap.takeRight(N.value)
          }
        }
      }
      sortedMap.takeRight(N.value).toIterator
    })
    val allTop10: Array[(Int, Iterable[String])] = partitions.groupByKey().collect()
    allTop10.foreach{
      case (k,v) =>{println(s"allTop10 = $k ${v.mkString(",")}")}
    }
    val finaltop10: SortedMap[Int, Iterable[String]] = SortedMap.empty[Int,Iterable[String]].++:(allTop10)
    val resultUsingMapPartition: SortedMap[Int, Iterable[String]] = finaltop10.takeRight(N.value)

    resultUsingMapPartition.foreach{
      case (k,v) => println(s"resultUsingMapPartition= $k \t ${v.mkString(",")}")
    }

    //第二种简单方式
    val createCombiner: Int => Int = (v:Int) => v
    val mergeValue: (Int, Int) => Int = (a:Int, b:Int) => a + b
    val moreConciseApproach: Array[(Int, Iterable[String])] = kv.combineByKey(createCombiner,mergeValue,mergeValue)
                              .map(_.swap).groupByKey().sortByKey(ascending = false).take(N.value)
    moreConciseApproach.foreach{
      case (k,v) => println(s"moreConciseApproach= $k \t ${v.mkString(",")}")
    }
    sc.stop()
  }
}
