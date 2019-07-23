import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  反转序列，相对频率计算
  */
object RelativeFrequency {
  def main(args: Array[String]): Unit = {
//    if (args.size < 3){
//      println("参数错误")
//      sys.exit(1)
//    }

    val sparkConf: SparkConf = new SparkConf().setAppName("RelativeFrequency").setMaster("local[10]")
    val  sc = new SparkContext(sparkConf)

//    val neighborWindow: Int = args(0).toInt
//    val input = args(1)
//    val output = args(2)

    val neighborWindow: Int = 2
    val input = "G:\\data\\spark\\RelativeFrequency.txt"
    val output = "G:\\data\\spark\\RelativeFrequency"

    val path: Path = new Path(output)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(output))){
      fs.delete(new Path(output), true)
    }

    val brodcastWindow: Broadcast[Int] = sc.broadcast(neighborWindow)

    val rawData: RDD[String] = sc.textFile(input)

    /**
      *  转换格式为:
      * (word, (neighbour, 1))
      */
    val pairs: RDD[(String, (String, Int))] = rawData.flatMap(line =>{
      val tokens: Array[String] = line.split(",")
      for{
        i <- 0 until tokens.length
        start = if (i - brodcastWindow.value < 0) 0 else i - brodcastWindow.value
        end = if (i + brodcastWindow.value >= tokens.length -1)  tokens.length - 1 else i + brodcastWindow.value
        j <- start to end if (j != i)
      } yield (tokens(i),(tokens(j),1))
    })
    pairs.foreach(f => println("pairs= "+f._1,f._2))


    //(word,sum(word))
    val totalByKey: RDD[(String, Int)] = pairs.map(t => (t._1,t._2._2)).reduceByKey(_+_)
    totalByKey.foreach(f => println("totalByKey= "+f._1,f._2))


    val grouped: RDD[(String, Iterable[(String, Int)])] = pairs.groupByKey()
    grouped.foreach(f => println("grouped= "+f._1,f._2))

    // (word, (neighbour, sum(neighbour)))
    val uniquePairs: RDD[(String, (String, Int))] = grouped.flatMapValues(_.groupBy(_._1).mapValues(_.unzip._2.sum))
    uniquePairs.foreach(f => println("uniquePairs= "+ f._1,f._2))

    // (word, ((neighbour, sum(neighbour)), sum(word)))
    val joined: RDD[(String, ((String, Int), Int))] = uniquePairs join totalByKey
    joined.foreach(f => println("joined= "+f._1,f._2))

    // ((key, neighbour), sum(neighbour)/sum(word))
    val relativeFrequency: RDD[((String, String), Double)] = joined.map(t =>{
      ((t._1,t._2._1._1), t._2._1._2.toDouble / t._2._2.toDouble)
    })
    relativeFrequency.foreach(f => println("relativeFrequency= "+f._1,f._2))

    // ((key, neighbour), relative_frequency)
    val formatResult_tab_separated: RDD[String] = relativeFrequency.map(t => t._1._1 + "\t" + t._1._2 + "\t"+ t._2)
    formatResult_tab_separated.foreach(f => println(f))

    formatResult_tab_separated.saveAsTextFile(output)

    sc.stop()
  }

}
