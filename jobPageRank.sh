#!/bin/sh
#自动针对每个职业子网络(同一职业一个单独的网络)进行pagerank迭代，每个子网络写入不同目录
for file in ./job/*
do
    if test -f $file
    then
        ../../../bin/spark-submit --class "sinaApp" --master spark://192.168.2.222:7077 --executor-memory 6G ../target/scala-2.10/sina-project_2.10-1.0.jar pageRank $file ./jobNet/$file
    fi
done
