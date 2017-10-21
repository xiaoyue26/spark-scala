package hbase.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by xiaoyue26 on 17/10/21.
  */
class AbstractApeHbaseJob {

  def clearOutPath(outPath: String): Unit = {
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    fileSystem.delete(new Path(outPath), true)
  }

  def initRdd(tableName: String): RDD[(ImmutableBytesWritable, Result)] = {
    val conf: SparkConf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)
    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set(TableInputFormat.SCAN_BATCHSIZE, Integer.toString(100))
    hbaseConf.set(TableInputFormat.SCAN_CACHEBLOCKS, "false")
    hbaseConf.set("hbase.zookeeper.quorum", AbstractApeHbaseJob.COMMON_HBASE_ZK)
    hbaseConf.set("zookeeper.znode.parent", AbstractApeHbaseJob.COMMON_HBASE_ZK_PARENT)
    val rdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rdd
  }

}

object AbstractApeHbaseJob {

  val COMMON_HBASE_ZK = "zk1,zk2,zk3,zk4,zk5"
  val COMMON_HBASE_ZK_PARENT = "/common-hbase"

}