package hbase.job

import hbase.parse.ApeUserQuestionStatParser

/**
  * Created by xiaoyue26 on 17/10/23.
  */
class ApeUserQuestionStatJob extends AbstractApeHbaseJob {

  import scala.collection.JavaConverters._

  def run(): Unit = {
    super.clearOutPath(ApeUserQuestionStatJob.HDFS_OUT_PATH)
    val rdd = super.initRdd(ApeUserQuestionStatJob.HTABLE_NAME)
    val parseResult = rdd.map {
      case (_, result) => {
        ApeUserQuestionStatParser.parse(result).asScala.toList
      }
    }.flatMap(x => x)
    parseResult.saveAsTextFile(ApeUserQuestionStatJob.HDFS_OUT_PATH)
  }

}

object ApeUserQuestionStatJob {

  val HDFS_OUT_PATH = "/user/fengmq01/test_user_question"
  val HTABLE_NAME = "ape_user_question_stat"

  def main(args: Array[String]): Unit = {
    val job = new ApeUserQuestionStatJob()
    job.run()
  }

}