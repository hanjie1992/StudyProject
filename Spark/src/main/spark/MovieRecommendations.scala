import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于电影内容的推荐
  */
object MovieRecommendations {
  def main(args: Array[String]): Unit = {
//    if (args.length < 2){
//      println("参数有误")
//      sys.exit(1)
//    }
    val sparkConf = new SparkConf().setAppName("").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)

//    val input = args(0)
//    val output = args(1)

    val input = "G:\\data\\spark\\movie.txt"
    val output = "G:\\data\\spark\\movie"

    val path: Path = new Path(output)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(output))){
      fs.delete(new Path(output), true)
    }

    val usersRatings: RDD[String] = sc.textFile(input)

    val userMovieRating: RDD[(String, String, Int)] = usersRatings.map(line =>  {
      val tokens: Array[String] = line.split(",")
      (tokens(0),tokens(1),tokens(2).toInt)
    })


    val numberOfRaterPerMovie: RDD[(String, Int)] = userMovieRating.map(umr => (umr._2,1)).reduceByKey(_+_)
//    numberOfRaterPerMovie.foreach(f => println("numberOfRaterPerMovie= "+f))

    val a: RDD[(String, (String, Int))] = userMovieRating.map(umr => (umr._2,(umr._1,umr._3)))
//    a.foreach(f => println("a=== "+f))
    val b: RDD[(String, ((String, Int), Int))] = userMovieRating.map(umr => (umr._2,(umr._1,umr._3))).join(numberOfRaterPerMovie)
//    b.foreach(f => println("b=== "+f))

    val userMovieRatingNumberOfRater: RDD[(String, String, Int, Int)] = userMovieRating.map(umr => (umr._2,(umr._1,umr._3))).join(numberOfRaterPerMovie)
        .map(tuple => (tuple._2._1._1,tuple._1,tuple._2._1._2,tuple._2._2))

//    userMovieRatingNumberOfRater.foreach(f => println("userMovieRatingNumberOfRater="+f))

    val groupedByUser: RDD[(String, Iterable[(String, Int, Int)])] = userMovieRatingNumberOfRater.map(f => (f._1,(f._2,f._3,f._4))).groupByKey()
    groupedByUser.foreach(f => println("groupedByUser= "+f))

    val moviePairs: RDD[((String, String), (Int, Int, Int, Int, Int, Int, Int))] = groupedByUser.flatMap(tuple => {
      val sorted: List[(String, Int, Int)] = tuple._2.toList.sortBy(f => f._1) //sorted by movies
      val tuple7: List[((String, String), (Int, Int, Int, Int, Int, Int, Int))] = for {
        movie1 <- sorted
        movie2 <- sorted
        if (movie1._1 < movie2._1);//避免重复
        ratingProuct = movie1._2 * movie2._2
        rating1Squared = movie1._2 * movie1._2
        rating2Squared = movie2._2 * movie2._2
      }yield {
        ((movie1._1,movie2._1),(movie1._2,movie1._3,movie2._2,movie2._3,ratingProuct,rating1Squared,rating2Squared))
      }
      tuple7
    })
    moviePairs.foreach(f => println("moviePairs= "+f))

    //groupbykey（）提供了一个昂贵的解决方案,您必须有足够的内存/RAM来保存.给定的键——否则可能会出现OOM错误），但是
    //CombineByKey（）和ReduceByKey（）将提供更好的扩展性能
    val moviePairsGrouped: RDD[((String, String), Iterable[(Int, Int, Int, Int, Int, Int, Int)])] = moviePairs.groupByKey()

    val result: RDD[((String, String), (Double, Double, Double))] = moviePairsGrouped.mapValues(itr => {
      val groupSize: Int = itr.size
      val (rating1,numOfRaters1,rating2,numOfRaters2,ratingProduct,rating1Squared,rating2Squared) =
        itr.foldRight((List[Int](),List[Int](),List[Int](),List[Int](),List[Int](),List[Int](),List[Int]()))((a,b)=>
          (
            a._1 :: b._1,
            a._2 :: b._2,
            a._3 :: b._3,
            a._4 :: b._4,
            a._5 :: b._5,
            a._6 :: b._6,
            a._7 :: b._7))

      val dotProduct: Int = ratingProduct.sum
      val rating1Sum: Int = rating1.sum
      val rating2Sum: Int = rating2.sum
      val rating1NormSq: Int = rating1Squared.sum
      val rating2NormSq: Int = rating2Squared.sum
      val maxNumOfumRaters1: Int = numOfRaters1.max
      val maxNumOfumRaters2: Int = numOfRaters2.max

      val numerator: Int = groupSize * dotProduct - rating1Sum * rating2Sum
      val denominator: Double = math.sqrt(groupSize * rating1NormSq - rating1Sum * rating1Sum) *
        math.sqrt(groupSize * rating2NormSq - rating2Sum * rating2Sum)
      val pearsonCorrelation: Double = numerator / denominator

      val cosineCorrelation: Double = dotProduct / (math.sqrt(rating1NormSq) * math.sqrt(rating2NormSq))

      val jaccardCorrelation: Double = groupSize.toDouble / (maxNumOfumRaters1 + maxNumOfumRaters2 - groupSize)

      (pearsonCorrelation, cosineCorrelation, jaccardCorrelation)

    })

    // for debugging purposes...
//    result.foreach(println)
    result.saveAsTextFile(output)
    // done
    sc.stop()
  }
}
