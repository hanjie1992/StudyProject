import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * kNN近邻算法
  * 1.计算已知类别数据集中的点与当前点之间的距离；
  * 2.按照距离递增次序排序；
  * 3.选取与当前点距离最小的k个点；
  * 4.确定前k个点所在类别的出现频率；
  * 5.返回前k个点所出现频率最高的类别作为当前点的预测分类
  */
object kNN {
  def main(args: Array[String]): Unit = {
//    if (args.length < 5){
//      println("参数有误")
//      sys.exit(1)
//    }

    val sparkConf: SparkConf = new SparkConf().setAppName("").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)

//    val k: Int = args(0).toInt //所选取的K个点
//    val d: Int = args(1).toInt //表示R和S向量维数的一个整数
//    val inputDatasetR = args(2)
//    val inputDatasetS = args(3)
//    val output = args(4)

    val k: Int = 2 //所选取的K个点
    val d: Int = 3 //表示R和S向量维数的一个整数
    val inputDatasetR = "G:\\data\\spark\\inputDatasetR.txt"
    val inputDatasetS = "G:\\data\\spark\\inputDatasetS.txt"
    val output = "G:\\data\\spark\\inputDataset.txt"

    val path: Path = new Path(output)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(output))){
      fs.delete(new Path(output), true)
    }

    val broadcastK: Broadcast[Int] = sc.broadcast(k)
    val broadcastD: Broadcast[Int] = sc.broadcast(d)

    val R: RDD[String] = sc.textFile(inputDatasetR)
    val S: RDD[String] = sc.textFile(inputDatasetS)

    //计算两点之间的距离
    def calculateDistance(rAsString: String, sAsString: String, d: Int): Double ={
      val r: Array[Double] = rAsString.split(",").map(_.toDouble)
      val s: Array[Double] = sAsString.split(",").map(_.toDouble)
      if (r.length != d || s.length !=d) Double.NaN else {
        math.sqrt((r,s).zipped.take(d).map{case (ri,si) => math.pow((ri - si),2)}.reduce(_+_))
      }
    }

    val cart: RDD[(String, String)] = R cartesian S

    val knnMapped: RDD[(String, (Double, String))] = cart.map(cartRecord => {
      val rRecord: String = cartRecord._1
      val sRecord: String = cartRecord._2
      val rTokens: Array[String] = rRecord.split(",")
      val rRecordID = rTokens(0)
      val r = rTokens(1)
      val sTokens: Array[String] = sRecord.split(",")
      val sClassificationID = sTokens(1)
      val s = sTokens(2)
      val distance: Double = calculateDistance(r,s,broadcastD.value)
      (rRecordID,(distance,sClassificationID))
    })

    val knnGrouped: RDD[(String, Iterable[(Double, String)])] = knnMapped.groupByKey()

    val knnOutput: RDD[(String, String)] = knnGrouped.mapValues(itr => {
      val nearestK: List[(Double, String)] = itr.toList.sortBy(_._1).take(broadcastK.value)
      val majority: Map[String, Int] = nearestK.map(f => (f._2,1)).groupBy(_._1).mapValues(list => {
        val (stringList,intlist) = list.unzip
        intlist.sum
      })
      majority.maxBy(_._2)._1
    })

    knnOutput.foreach(println)
    knnOutput.saveAsTextFile(output)

    sc.stop()
  }
}
