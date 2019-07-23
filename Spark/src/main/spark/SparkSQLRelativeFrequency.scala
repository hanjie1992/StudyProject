import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Spark SQL实现反转序列，相对频率计算
  */
object SparkSQLRelativeFrequency {
  def main(args: Array[String]): Unit = {

//    if (args.length < 3){
//      println("参数错误")
//      sys.exit(1)
//    }

    val sparkConf: SparkConf = new SparkConf().setAppName("SparkSQLRelativeFrequency").setMaster("local[10]")
    val spark: SparkSession = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate()

    val sc: SparkContext = spark.sparkContext

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

    //Schema (word, neighbour, frequency)
    val rfSchema = StructType(Seq(
      StructField("word",StringType,false),
      StructField("neighbour",StringType,false),
      StructField("frequency",IntegerType,false)
    ))

    val rowRDD: RDD[Row] = rawData.flatMap(line =>{
      val tokens: Array[String] = line.split(",")
      for {
        i <- 0 until  tokens.length
        start = if (i - brodcastWindow.value < 0) 0 else i - brodcastWindow.value
        end = if (i + brodcastWindow.value >= tokens.length) tokens.length - 1 else i + brodcastWindow.value
        j <- start to end if (j != i)
      } yield Row(tokens(i),tokens(j),1)
    })

    val rfDataFrame: DataFrame = spark.createDataFrame(rowRDD,rfSchema)
    rfDataFrame.createOrReplaceTempView("rfTable")

    import spark.sql

    val query: String = "select a.word,a.neighbour,(a.feq_total/b.total) rf " +
      "from (select word,neighbour,sum(frequency) feq_total from rfTable group by word,neighbour) a " +
      "inner join (select word,sum(frequency) as total from rfTable group by word) b on a.word = b.word"

    val sqlResult: DataFrame = sql(query)
    sqlResult.show()//展示20行
//    sqlResult.write.save(output+"-parquetFormat")//压缩parquet格式保存输出，用于大型项目
    sqlResult.rdd.saveAsTextFile(output+"-textFormat")//文本格式保存


  }
}
