import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

/**
  * 查找共同好友
  */
object FindCommonFriends {

  def main(args: Array[String]): Unit = {
//    if (args.length < 2){
//      println("参数错误")
//      sys.exit(1)
//    }

    val sparkConf: SparkConf = new SparkConf().setAppName("FindCommonFriends").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)

//    val input = args(0)
//    val output = args(1)

    val input = "G:\\data\\spark\\CommonFriends.txt"
    val output = "G:\\data\\spark\\CommonFriends"

    val path: Path = new Path(output)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(output))){
      fs.delete(new Path(output), true)
    }

    val records: RDD[String] = sc.textFile(input)

    val pairs: RDD[((Long, Long), List[Long])] = records.flatMap(s => {
      val tokens: Array[String] = s.split(",")
      val person: Long = tokens(0).toLong
      val friends: List[Long] = tokens(1).split("\\s+").map(_.toLong).toList
//      friends.foreach(f => println("friends= "+f))
      val result: immutable.IndexedSeq[((Long, Long), List[Long])] = for {
        i <- 0 until friends.size
        friend= friends(i)
      }yield {
        if (person < friend)
          ((person, friend), friends)
        else
          ((friend, person), friends)
      }
//      result.foreach(f => println("result= "+f._1,f._2))
      result
    })


    val grouped: RDD[((Long, Long), Iterable[List[Long]])] = pairs.groupByKey()
//    grouped.foreach(f => println("grouped = "+f._1,f._2))
    val commonFriends: RDD[((Long, Long), immutable.Iterable[Long])] = grouped.mapValues(iter => {
      val friendCount: Iterable[(Long, Int)] = for {
        list <- iter
        if !list.isEmpty
        friend <- list
      }yield ((friend,1))
//      friendCount.foreach(f => println("friendCount= "+f._1,f._2))
      friendCount.groupBy(_._1).mapValues(_.unzip._2.sum).filter(_._2 > 1).map(_._1)
    })
    commonFriends.saveAsTextFile(output)
    val formatedResult: RDD[String] = commonFriends.map(
      f => s"(${f._1._1},${f._1._2})\t${f._2.mkString("[",",","]")}"
    )
    formatedResult.foreach(println)
    sc.stop()
  }
}
