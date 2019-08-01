import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算并识别给定图形的三角形
  */
object CountTriangles {
  def main(args: Array[String]): Unit = {
//    if (args.length < 2){
//      println("参数错误")
//      sys.exit(1)
//    }

    val sparkConf: SparkConf = new SparkConf().setAppName("").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)

//    val input = args(0)
//    val output = args(1)

    val input = "G:\\data\\spark\\CountTriangles.txt"
    val output = "G:\\data\\spark\\CountTriangles"

    val path: Path = new Path(output)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(output))){
      fs.delete(new Path(output), true)
    }

    val lines: RDD[String] = sc.textFile(input)

    val edges: RDD[(Long, Long)] = lines.flatMap(line => {
      val tokens: Array[String] = line.split(",")
      val start: Long = tokens(0).toLong
      val end: Long = tokens(1).toLong
      (start,end) :: (end,start) :: Nil
    })
//    edges.foreach(f => println("edges= "+f))

    val triads: RDD[(Long, Iterable[Long])] = edges.groupByKey()
//    triads.foreach(f => println("triads= "+f))


    val possibleTriads: RDD[((Long, Long), Long)] = triads.flatMap(tuple => {
      val values: List[Long] = tuple._2.toList
      val result: List[((Long, Long), Long)] = values.map(v => {
        ((tuple._1,v),0l)
      })
//      result.foreach(f => println("result= "+f))
      val sorted: List[Long] = values.sorted
//      sorted.foreach(f => println("sorted== "+f))
      val  combinations: List[(Long, Long)] = sorted.combinations(2).map{case Seq(a,b) => (a,b)}.toList
//      combinations.foreach( f => println("combinations= "+f))
      val a = combinations.map((_,tuple._1))
//      a.foreach(f => println("a===== "+f))
      combinations.map((_,tuple._1)) ::: result
    })

//    possibleTriads.foreach(f => println("possibleTriads= "+f))

    val triadsGrouped: RDD[((Long, Long), Iterable[Long])] = possibleTriads.groupByKey()
//    triadsGrouped.foreach(f => println("triadsGrouped== "+f))

    val trianglesWithDuplicates: RDD[(Long, Long, Long)] = triadsGrouped.flatMap(tg => {
      val key: (Long, Long) = tg._1
      val values: Iterable[Long] = tg._2
      val list: Iterable[Long] = values.filter(_!= 0)
      if (values.exists(_==0)){
        if (list.isEmpty) Nil
        list.map(a => {
          val sortedTriangle: List[Long] = (key._1 :: key._2 :: a :: Nil).sorted
          sortedTriangle.foreach(f => println("sortedTriangle== "+f))
          (sortedTriangle(0),sortedTriangle(1),sortedTriangle(2))
        })
      }else Nil
    })
//    trianglesWithDuplicates.foreach(f => println("trianglesWithDuplicates== "+f))

    val uniqueTriangles: RDD[(Long, Long, Long)] = trianglesWithDuplicates distinct

    uniqueTriangles.foreach(println)

    uniqueTriangles.saveAsTextFile(output)

    sc.stop()
  }
}
