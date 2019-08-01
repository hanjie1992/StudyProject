import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 朴素贝叶斯算法
  * input query data是一个新数据，我们希望通过使用/应用构建的naive bayes分类器对其进行分类。
  * input pt是由NaiveBayesClassifier类生成的概率表
  * input classes是由NaiveBayesClassifier类生成的类吗？
  * output path是由构建的naive bayes分类器分类的新数据的输出
  *
  */
object NaiveBayesClassifier {
  def main(args: Array[String]): Unit = {
//    if (args.length < 4){
//      println("参数有误")
//      sys.exit(1)
//    }

    val sparkConf: SparkConf = new SparkConf().setAppName("").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)

//    val input = args(0)
//    val nbProbailityTablePath = args(1)
//    val classesPath = args(2)
//    val output = args(3)

    val input = "G:\\data\\spark\\NaiveBayes.txt"
    val nbProbailityTablePath = "G:\\data\\spark\\NaiveBayesClassProbaility.txt"
    val classesPath = "G:\\data\\spark\\NaiveBayesClass.txt"
    val output = "G:\\data\\spark\\NaiveBayes.txt"

//    val a = nbProbailityTablePaths

    val path: Path = new Path(output)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(output))){
      fs.delete(new Path(output), true)
    }
    val newdata: RDD[String] = sc.textFile(input)
    val classifierRDD: RDD[((String, String), Double)] = sc.objectFile[Tuple2[Tuple2[String,String],Double]](nbProbailityTablePath)

    val classifier: collection.Map[(String, String), Double] = classifierRDD.collectAsMap()
    val broadcastClassifier: Broadcast[collection.Map[(String, String), Double]] = sc.broadcast(classifier)
    val classesRDD: RDD[String] = sc.textFile(classesPath)
    val broadcastClasses: Broadcast[Array[String]] = sc.broadcast(classesRDD.collect())

    val classified: RDD[(String, String)] = newdata.map(rec => {
      val classifier: collection.Map[(String, String), Double] = broadcastClassifier.value
      val classes: Array[String] = broadcastClasses.value
      val attributes: Array[String] = rec.split(",")

      val class_score: Array[(String, Double)] = classes.map(aClass => {
        val posterior: Double = classifier.getOrElse(("CLASS",aClass),1d)
        val probabilities: Array[Double] = attributes.map(attributes => {
          classifier.getOrElse((attributes,aClass),0d)
        })
        (aClass,probabilities.product * posterior)
      })
      val maxClass: (String, Double) = class_score.maxBy(_._2)
      (rec,maxClass._1)
    })

    classified.foreach(println)

    classified.saveAsTextFile(output)

    sc.stop()
  }
}
