#!/usr/bin/env bash

cd ..
gradle fatjar
mupload -f /Users/xiaoyue26/IdeaProjects/spark-scala/build/libs/fatjar.jar -o fengfat.jar