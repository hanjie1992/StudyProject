import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  *  演示如何在两个RDD上执行“左外部联接”,使用Spark的内置功能“LeftOuterJoin”
  */
object SparkLeftOuterJoin {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)

    //    val usersInputFile = args(0)
    //    val transactionsInputFile = args(1)
    //    val output = args(2)
    val usersInputFile = "G:\\data\\spark\\users.txt"
    val transactionsInputFile = "G:\\data\\spark\\transactions.txt"
    val output = "G:\\data\\spark\\SparkLeftJoinDriver"

    val path: Path = new Path(output)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(output))){
      fs.delete(new Path(output), true)
    }

    val usersRaw: RDD[String] = sc.textFile(usersInputFile)
    val transactionsRaw: RDD[String] = sc.textFile(transactionsInputFile)

    val users: RDD[(String, String)] =usersRaw.map(line =>{
      val tokens: Array[String] = line.split(",")
      (tokens(0),tokens(1))
    })
    users.foreach(f => println("users= "+(f._1,f._2)+","))

    val transactions: RDD[(String, String)] = transactionsRaw.map(line =>{
      val tokens: Array[String] = line.split(",")
      (tokens(2),tokens(1))
    })
    transactions.foreach(f => println("transactions= "+(f._1,f._2)))

    val joined: RDD[(String, (String, Option[String]))] = transactions leftOuterJoin users

    joined.foreach(f => println("joined= "+f._1,f._2._1,f._2._2))

    //getorelse是一个方法availbale on选项，它返回值（如果存在）或返回传递值（在本例中未知）
    val productLocations: RDD[(String, String)] = joined.values.map(f => (f._1,f._2.getOrElse("unknown")))
    productLocations.foreach(f => println("productLocations= "+f._1,f._2))


    val productByLocations: RDD[(String, Iterable[String])] = productLocations.groupByKey()
    productByLocations.foreach(f => println("productByLocations= "+f._1,f._2))

    val productWithUniqueLocations: RDD[(String, Set[String])] = productByLocations.mapValues(_.toSet)//转换为set集合将删除重复项
    productWithUniqueLocations.foreach{
      case (k,v) => println(s"productWithUniqueLocations= $k,${v}")
    }

    val result: RDD[(String, Int)] = productWithUniqueLocations.map(t => (t._1,t._2.size))//返回（产品、位置计数）元组。
    result.foreach(f => println("result= "+f._1,f._2))
    result.coalesce(1)
    result.saveAsTextFile(output)

    sc.stop()

  }
}
