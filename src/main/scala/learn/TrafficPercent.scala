package learn

/**
  * Created by xiaoyue26 on 17/8/18.
  */

import org.apache.hadoop.fs.{ContentSummary, FileSystem, Path}
import org.apache.spark._
import org.apache.spark.sql.{Row, SQLContext}

object TrafficPercent {
  def filterRow(row: Row): Boolean = {
    //      return true
    val esLatency = row.getAs[Float]("eslatency")
    val eslossrate = row.getAs[Float]("eslossrate")
    val rplatency = row.getAs[Float]("rplatency")
    val rplossrate = row.getAs[Float]("rplossrate")
    if (esLatency >= 0 && eslossrate >= 0 && eslossrate <= 100
      && rplatency >= 0 && rplossrate >= 0 && esLatency < 15 * 1000
    ) {
      return true
    }
    else {
      return false
    }
  }

  def getPair(row: Row): ((String, String), Float) = {
    val key1 = row.getAs[String]("ipoperator")
    val key2 = row.getAs[String]("ipprovince")
    val value = row.getAs[Float]("eslossrate")
    return ((key1, key2), value)

  }

  def testAvg(): Unit = {
    val conf = new SparkConf().setAppName("TrafficPercent")
    val sc = new SparkContext(conf)
    //val hiveCtx = HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    val lines = sqlContext.sql(" SELECT * FROM tutor.ods_tutor_live_traffic_scheduler WHERE dt='2017-08-16'")
    //lines.printSchema()
    val linerdd = lines.rdd
    var col1: String = linerdd.first().getAs("dt")
    println(col1)

    // 获得 key-value

    val kvrdd = linerdd.filter(filterRow).map((x) => getPair(x))
    kvrdd.first()

    // get avg
    def wordCount(x: Float): (Float, Int) = {
      return (x, 1)
    }

    def pairAdd(a: (Float, Int), b: (Float, Int)): (Float, Int) = {
      return (a._1 + b._1, a._2 + b._2)
    }

    val res = kvrdd.mapValues(wordCount).reduceByKey(pairAdd)

    val firstRes = res.first()

    def extract(x: (Float, Int)): Float = {
      return x._1 / x._2
    }

    println(firstRes._1, extract(firstRes._2))

    /*outRdd.saveAsTextFile("/user/fengmq01/output")
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://f04"), sc.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path("/user/fengmq01/output"),true)*/
    sc.stop()
  }

  def test2(): Unit = {

  }

  def getContentSummary(f: Path, hdfs: FileSystem): ContentSummary = {
    val status = hdfs.getFileStatus(f)
    if (status.isFile) { // f is a file
      return new ContentSummary(status.getLen, 1, 0)
    }
    // f is a directory
    val summary = new Array[Long](3)
    summary(0) = 0
    summary(1) = 0
    summary(2) = 1
    for (s <- hdfs.listStatus(f)) {
      var c: ContentSummary = null
      if (s.isDirectory) {
        c = getContentSummary(s.getPath, hdfs)
        println("heeeeeeeeeeeeeeer")
        println(s)
      } else {
        c = new ContentSummary(s.getLen, 1, 0)
      }
      summary(0) += c.getLength
      summary(1) += c.getFileCount
      summary(2) += c.getDirectoryCount
    }
    new ContentSummary(summary(0), summary(1), summary(2))
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TrafficPercent")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val lines = sqlContext.sql(" SELECT * FROM tutor.ods_tutor_live_traffic_scheduler WHERE dt='2017-08-16'")
    //lines.printSchema()
    val linerdd = lines.rdd

    val filePath = "/log/frog/year=2017/month=09/day=20"
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val files = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(filePath))
    for (file <- files) {
      println(file.isDirectory)
    }
    val cs = getContentSummary(new Path(filePath), hdfs)
    println(cs.getDirectoryCount)
    println(cs.getSpaceConsumed)
    hdfs.rename(new Path("/user/fengmq01/test3/part-m-00000_duplicate_1506057925305"),new Path("/tmp/1"))


  }
}

// scalastyle:on println