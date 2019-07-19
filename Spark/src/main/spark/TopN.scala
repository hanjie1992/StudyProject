import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap

/**
  * 求topN k唯一的情况
  */

object TopN {
  def main(args: Array[String]): Unit = {
//    if (args.size < 1){
//      println("输入格式不对")
//      sys.exit(1)
//    }
    val sparkConf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)

    val N = sc.broadcast(5)
//    val path = args(0)
    val icIn: String = "G:\\data\\spark\\catTopN.txt"
    val input: RDD[String] = sc.textFile(icIn)
    val pair: RDD[(Int, Array[String])] = input.map(line => {
      val tokens: Array[String] = line.split(",")
      (tokens(2).toInt, tokens)
    })
    import Ordering.Implicits._
    val partitions: RDD[(Int, Array[String])] = pair.mapPartitions(itr => {
      var sortedMap: SortedMap[Int, Array[String]] = SortedMap.empty[Int,Array[String]]
      itr.foreach{ tuple =>
        {
          sortedMap += tuple
          if (sortedMap.size > N.value){
            sortedMap = sortedMap.takeRight(N.value)
          }
        }
      }
      sortedMap.takeRight(N.value).toIterator
    })

    val alltop10: Array[(Int, Array[String])] = partitions.collect()

    alltop10.foreach{
      case (k,v) => println(s"alltop10= $k ${v.asInstanceOf[Array[String]].mkString(",")}")
    }

    val finaltop10: SortedMap[Int, Array[String]] = SortedMap.empty[Int,Array[String]].++:(alltop10)
    val resultUsingMapPartition: SortedMap[Int, Array[String]] = finaltop10.takeRight(N.value)

    resultUsingMapPartition.foreach{
      case (k,v) => println(s"resultUsingMapPartition = $k \t ${v.asInstanceOf[Array[String]].mkString(",")}")
    }

    val moreConciseApproch: Array[(Int, Iterable[Array[String]])] = pair.groupByKey().sortByKey(false).take(N.value)
    moreConciseApproch.foreach{
      case (k,v) => println(s" moreConciseApproch = $k ${v.flatten.mkString(",")}")
    }
    sc.stop()
  }
}
