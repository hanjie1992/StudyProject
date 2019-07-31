import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD
/**
  * KMeans(K-均值聚类算法)
  * 使用Spark的MLlib
  */
object ScalaKMeans {

  def main(args: Array[String]): Unit = {
//    if (args.length < 3){
//      println("参数错误")
//      sys.exit(1)
//    }

    val sparkConf: SparkConf = new SparkConf().setAppName("").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)

//    val input = args(0)
//    val k: Int = args(1).toInt //期望的族数
//    val iterations: Int = args(2).toInt //最大迭代次数
//    val runs: AnyVal = if (args.length >= 3) args(3).toInt //k均值算法的次数

    val input = "G:\\data\\spark\\ScalaKMeans.txt"
    val k: Int = 3.toInt //期望的族数
    val iterations: Int = 10.toInt //最大迭代次数
    val runs: AnyVal = if (args.length >= 3) args(3).toInt //k均值算法的次数

    val lines: RDD[String] = sc.textFile(input)

    //构建向量点
    val points: RDD[linalg.Vector] = lines.map(line => {
      val tokens: Array[String] = line.split(",")
      Vectors.dense(tokens.map(_.toDouble))
    })

    //构建模型
    //val model = KMeans.train(points, k, iterations, runs, KMeans.K_MEANS_PARALLEL)
    val model: KMeansModel = KMeans.train(points,k,iterations,KMeans.K_MEANS_PARALLEL)

    println("集群中心:")
    model.clusterCenters.foreach(println)

    //计算成本
    val cost: Double = model.computeCost(points)
    println(s"成本：${cost}")

    sc.stop()
  }
}
