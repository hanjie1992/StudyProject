import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkPhoneOdData {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkPhoneOdData").setMaster("local[10]")
    val sc: SparkContext = new SparkContext(sparkConf)
    val icIn: String = "G:\\data\\od_20181210.txt"
//    val icIn: String = "G:\\data\\22.txt"
    val icOut: String = "G:\\data\\out\\spark"
    val path: Path = new Path(icOut)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(new Path(icOut))){
      fs.delete(new Path(icOut), true)
    }
    val df: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val lineFile :RDD[String] = sc.textFile(icIn)
    val  result: Unit =  lineFile.map(line => line.split(","))
      .filter(t=>(t(1).compareTo("2018-12-10 05:30:00")>0) && (t(1).compareTo("2018-12-10 09:00:00")<0))
//      .foreach((x:(Array[String]))=>x.mkString(" ") )
      .map(x=>(x(2),1)).reduceByKey(_+_).take(100)
      .foreach((x:(String,Int))=>println(x+"   "))

//      .coalesce(1,shuffle = true).saveAsTextFile(icOut)
//      .map(x=>(x(2),x))
//    lineFile.flatMap(line => line.split(",")).map(line => line.split(",")(0)).coalesce(1,shuffle = true).saveAsTextFile(icOut)
//    val lineF: RDD[(String, Array[String])] = lineFile.map(line => line.split(",")).map(x=>(x(1),x))
////      .map{case t=>(t(1),t)}
//    val a: RDD[String] = lineF.map(_._1)
//    a.foreach((x:String)=>println(x+""))




//      .map{case Array(x,y)=>(x,y)}
//      .map(_._1)
//    val newRdd = lineFile.map( row => (row._1, row._2) )
//    lineF.map{case Array(x,y)=>(x,y)}.collect()
//    print(lineF.toString())




//    val lineList: RDD[String] = lineFile.map(line => line.split(",")(1))
//    lineF.filter(TimeCompare(lineList,"2018-12-10 05:00:00","2018-12-10 08:30:00"))

//    println(lineFile.map(line => line.split(",")(2)).distinct().count())//620
//    lineFile.map(line => line.split(",")(2)).distinct().coalesce(1,true).saveAsTextFile(icOut)

  }


  def TimeCompare(timeData:RDD[String],startTimeData:String,endTimeData:String): Boolean ={
    var flag=false
    val df: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    try {
        val curr:Date=df.parse(timeData.toString())
        val star: Date = df.parse(startTimeData)
        val end: Date = df.parse(endTimeData)
        val cs: Long = curr.getTime - star.getTime
        val ec: Long = end.getTime - curr.getTime
        if (cs >= 0 &&  ec>0) {
          flag=true
        }
    }
    catch {
      case _: Exception =>
    }
    flag
  }

}
