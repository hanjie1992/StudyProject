import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 此方法使用了DataFrame插件。DataFrame功能强大，易于使用，推荐使用
  */
object DataFrameLeftOuterJoin {
  def main(args: Array[String]): Unit = {

//    val usersInputFile = args(0)
//    val transactionsInputFile = args(1)
//    val output = args(2)

    val usersInputFile = "G:\\data\\spark\\users.txt"
    val transactionsInputFile = "G:\\data\\spark\\transactions.txt"
    val output = "G:\\data\\spark\\SparkLeftJoinDriver"

    val sparkConf = new SparkConf()

    /**
      * 1.Spark 1.6.2 or 以下，用此方式
      *   val sc = new SparkContext(sparkConf)
      *   val spark = new SQLContext(sc)
      * 2.Spark 2.0.0，用程序代码方式
      */
    val spark: SparkSession = SparkSession
        .builder()
        .appName("DataFrameLeftOuterJoin")
        .master("local[10]")
        .config(sparkConf)
        .getOrCreate()

    //Spark 2.0.0
    val sc: SparkContext = spark.sparkContext

    val path: Path = new Path(output)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(output))){
      fs.delete(new Path(output), true)
    }

    import spark.implicits._
    import org.apache.spark.sql.types._

    val userSchema = StructType(Seq(
      StructField("userId", StringType, false),
      StructField("location", StringType, false)
    ))

    val transactionSchema = StructType(Seq(
      StructField("transactionId",StringType,false),
      StructField("productId",StringType,false),
      StructField("userId",StringType,false),
      StructField("quantity",StringType,false),
      StructField("price",StringType,false)
    ))
    def userRows(line: String): Row = {
      val tokens: Array[String] = line.split(",")
      Row(tokens(0), tokens(1))
    }

    def transactionRows(line: String):Row ={
      val tokens: Array[String] = line.split(",")
      Row(tokens(0),tokens(1),tokens(2),tokens(3),tokens(4))
    }

    val usersRaw: RDD[String] = sc.textFile(usersInputFile)
    val userRDDRows: RDD[Row] = usersRaw.map(userRows(_))//转换为RDD
    val users: DataFrame = spark.createDataFrame(userRDDRows,userSchema)

    val transactionRaw: RDD[String] = sc.textFile(transactionsInputFile)
    val transactionsRDDRows: RDD[Row] = transactionRaw.map(transactionRows(_))//转换为RDD
    val transactions: DataFrame = spark.createDataFrame(transactionsRDDRows,transactionSchema)

    //方法一：DataFrame API
//    val joined: DataFrame = transactions.join(users,transactions("userId") === users("userId"))
//    joined.printSchema()
//    val product_location: DataFrame = joined.select(joined.col("productId"),joined.col("location"))//仅选择ProductID和位置
//    val product_location_distinct: Dataset[Row] = product_location.distinct()
//    val products: DataFrame = product_location_distinct.groupBy("productId").count()
//    products.show()//在控制台上打印前20条记录
//    products.write.save(output+"/approach1")//以压缩格式保存输出，建议用于大型项目。
//    products.rdd.saveAsTextFile(output+"approach1_textFormat")//将DataFram转换为rdd[行]并将其保存到文本文件中

    //方法二：SQL查询
    // Use below for Spark 1.6.2 or below
    // users.registerTempTable("users") // Register as table (temporary) so that query can be performed on the table
    // transactions.registerTempTable("transactions") // Register as table (temporary) so that query can be performed on the table
    /**
      * Spark 1.6.2 or 以下，用注释的方法写
      * users.registerTempTable("users")
      * transactions.registerTempTable("transactions")
      * Spark 2.0.0 用程序方法写
      */
    users.createOrReplaceTempView("users")//注册为表（临时），以便可以对表执行查询
    transactions.createOrReplaceTempView("transactions")

    import  spark.sql

    val sqlResult: DataFrame = sql("select productId,count(distinct location) locCount from transactions left join users " +
      "on transactions.userId = users.userId group by productId")
    sqlResult.show()
    sqlResult.write.save(output+"-approach2")
    sqlResult.rdd.saveAsTextFile(output+"-approach2_textFormat")

    spark.stop()

  }
}
