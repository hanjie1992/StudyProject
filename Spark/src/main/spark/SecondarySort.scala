import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序
  *
  */
object SecondarySort {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("SecondarySort").setMaster("local[10]")
    val sc = new SparkContext(config)

    //    if (args.length !=3){
//      println("输入格式错误")
//      sys.exit(1)
//    }
//    val partitions = args(0).toInt
//    val inputPath = args(1)
//    val outputPath = args(2)
    val partitions = 1
    val inputPath = "G:\\data\\spark\\SeconderSort.txt"
    val outputPath = "G:\\data\\spark\\SeconderSort"
    val path: Path = new Path(outputPath)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(outputPath))){
      fs.delete(new Path(outputPath), true)
    }
    val input = sc.textFile(inputPath)

    val valueToKey = input.map(x =>{
      val line = x.split(",")
      ((line(0) +"-" + line(1),line(2).toInt),line(2).toInt)
    })
    implicit def tupleOrderingDesc = new Ordering[Tuple2[String,Int]]{
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        if (y._1.compare(x._1)==0){
          y._2.compare(x._2)
        }else y._1.compare(x._1)
      }
    }
    val sorted: RDD[((String, Int), Int)] = valueToKey.repartitionAndSortWithinPartitions(new CustomPartitioner(partitions))

    val result = sorted.map{
      case (k,v) =>(k._1,v)
    }
    result.saveAsTextFile(outputPath)
    sc.stop()
  }
}
