#!/usr/bin/env bash
spark-submit --master yarn-cluster --queue default \
--class SparkPi --name test --num-executors 1 --executor-memory 4g \
/Users/xiaoyue26/IdeaProjects/spark-scala/build/libs/fatjar.jar
