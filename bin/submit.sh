#!/usr/bin/env bash
# gradle fatjar
# 1. SparkPi
:<<! # 多行注释
spark-submit --master yarn-cluster --queue default \
--class SparkPi --name test --num-executors 1 --executor-memory 4g \
/Users/xiaoyue26/IdeaProjects/spark-scala/build/libs/fatjar.jar
!


# 2. HdfsWordCount
spark-submit --master yarn-cluster --queue default \
--class HdfsWordCount --name test --num-executors 1 --executor-memory 4g \
/Users/xiaoyue26/IdeaProjects/spark-scala/build/libs/fatjar.jar /user/fengmq01/test2