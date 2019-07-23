import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * 西安信令数据分析
  */
object XiAnSparkPhoneOdData {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("XiAnSparkPhoneOdData").setLevel(Level.WARN)
    val sparkConf :SparkConf = new SparkConf().setMaster("local[10]").setAppName("XiAnSparkPhoneOdData")
    val sc: SparkContext = new SparkContext(sparkConf)
    val spark :SparkSession = SparkSession.builder().master("local[10]").appName("XiAnSparkPhoneOdDataSql").enableHiveSupport().getOrCreate()
    val icIn: String = "G:\\西安数据\\信令数据\\od\\od_20181210.txt"
    val icOut: String = "G:\\data\\out\\spark"
    val path: Path = new Path(icOut)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(icOut))){
      fs.delete(new Path(icOut), true)
    }
    val lineFile :RDD[String] = sc.textFile(icIn)
    //第一种方式
//    case class Phone(id:String,startTime:String,startPlace:String,endTime:String,endPlace:String,drivingTime:String)
//    import spark.implicits._
//    val phone: DataFrame = lineFile.map(_.split(",")).map(p=>Phone(p(0),p(1),p(2),p(3),p(4),p(5))).toDF()
//    phone.createOrReplaceTempView("phone")

//第二种方式
    val schemaString = "id,start_time,start_region_name,end_time,end_region_name,travel_time"
    val schema = StructType(schemaString.split(",").map( fieldName => StructField(fieldName,StringType,nullable = true)))
    val rowRdd: RDD[Row] = lineFile.map(line => line.split(",")).map(p=>Row(p(0),p(1),p(2),p(3),p(4),p(5)))
    val daDataF: DataFrame = spark.createDataFrame(rowRdd,schema)
    getTravelNameAnalysis(spark,daDataF).show()
//    importDataToMySQL("phone",result)
  }
  def getTravelNameAnalysis(sparkSession: SparkSession,data:DataFrame): DataFrame ={
    data.createOrReplaceTempView("app_travel_od")
    val sqlStr: String =
      s"""
         | select id,start_time,start_region_name,end_time,end_region_name,travel_time from app_travel_od
       """.stripMargin
    sparkSession.sql(sqlStr)
  }

  def importDataToMySQL(table: String, data: DataFrame): Unit = {
    data.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", System.getProperty("url"))
      .option("user", System.getProperty("user"))
      .option("password", System.getProperty("password"))
      .option("dbtable", table)
      .save()
  }
}
