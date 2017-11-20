#!/usr/bin/env bash
# gradle fatjar
# 1. SparkPi
:<<! # 多行注释
spark-submit --master yarn-cluster --queue default \
--class SparkPi --name test --num-executors 1 --executor-memory 4g \
/Users/xiaoyue26/IdeaProjects/spark-scala/build/libs/fatjar.jar
!


# 2. HdfsWordCount
:<<!
spark-submit --master yarn-cluster --queue default \
--class HdfsWordCount --name test --num-executors 1 --executor-memory 4g \
/Users/xiaoyue26/IdeaProjects/spark-scala/build/libs/fatjar.jar /user/fengmq01/test2
!

# 3. traffic Percent
:<<!
spark-submit --master yarn-cluster --queue default \
--class TrafficPercent --name TrafficPercent --num-executors 1 --executor-memory 4g \
/Users/xiaoyue26/IdeaProjects/spark-scala/build/libs/fatjar.jar
!

# 4. ApeQuestionStatJob
:<<!
set -ex
ssh dx-pipe-cpu1-pm
cd fengmq
wget http://upload.zhenguanyu.com/uploads/fengfat.jar
spark-submit --master yarn-cluster --queue default \
--class hbase.job.ApeQuestionStatJob --name ApeQuestionStatJob --num-executors 30 --executor-memory 4g \
fengfat.jar
!

# 5. ApeUserQuestionStatJob
:<<!
set -ex
ssh dx-pipe-cpu1-pm
cd fengmq
wget http://upload.zhenguanyu.com/uploads/fengfat.jar
spark-submit --master yarn-cluster --queue default \
--class hbase.job.ApeUserQuestionStatJob --name ApeUserQuestionStatJob --num-executors 1000 --executor-memory 4g \
fengfat.jar
!


