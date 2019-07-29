import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

/**
  * 购物篮分析
  */
object MarketBasketAnalysis {
  def main(args: Array[String]): Unit = {
//    if (args.size < 2){
//      println("参数错误")
//      sys.exit(1)
//    }

    val sparkConf: SparkConf = new SparkConf().setAppName("").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)

//    val input = args(0)
//    val output = args(1)

    val input = "G:\\data\\spark\\MarketBasketAnalysis.txt"
    val output = "G:\\data\\spark\\MarketBasketAnalysis"

    val path: Path = new Path(output)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(output))){
      fs.delete(new Path(output), true)
    }

    val transactions: RDD[String] = sc.textFile(input)

    val patterns: RDD[(List[String], Int)] = transactions.flatMap(line => {
      val items: List[String] = line.split(",").toList
      items.foreach(f => println("items= "+f))
      (0 to items.size) flatMap items.combinations filter(xs => !xs.isEmpty)
    }).map((_,1))
    patterns.foreach(f => println("patterns= "+f))

    val combined: RDD[(List[String], Int)] = patterns.reduceByKey(_+_)
    combined.foreach(f => println("combined= "+f))

    val subpatterns: RDD[(List[String], (List[String], Int))] = combined.flatMap(pattern => {
      val result: ListBuffer[(List[String], (List[String], Int))] = ListBuffer.empty[Tuple2[List[String],Tuple2[List[String],Int]]]
      result += ((pattern._1,(Nil,pattern._2)))

      val sublist: immutable.IndexedSeq[(List[String], (List[String], Int))] = for {
        i <- 0 until pattern._1.size
        xs = pattern._1.take(i) ++ pattern._1.drop(i + 1)
        if xs.size > 0
      } yield (xs,(pattern._1,pattern._2))
      result ++= sublist
      result.toList
    })

    val rules: RDD[(List[String], Iterable[(List[String], Int)])] = subpatterns.groupByKey()

    val assocRules: RDD[List[(List[String], List[String], Double)]] = rules.map(in => {
      val fromCount: (List[String], Int) = in._2.find(p => p._1 == Nil).get
      val toList: List[(List[String], Int)] = in._2.filter(p => p._1 != Nil).toList
      if (toList.isEmpty) Nil
      else {
        val result: List[(List[String], List[String], Double)] =
          for {
            t2 <- toList
            confidence = t2._2.toDouble / fromCount._2.toDouble
            difference = t2._1 diff in._1
          }yield ((in._1,difference,confidence))
        result
      }
    })

    //格式化结果以便于阅读
    val formatResult: RDD[(String, String, Double)] = assocRules.flatMap(f => {
      f.map(s => (s._1.mkString("[", ",", "]"),s._2.mkString("[",",","]"),s._3))
    })
    formatResult.saveAsTextFile(output)
    sc.stop()

  }
}
