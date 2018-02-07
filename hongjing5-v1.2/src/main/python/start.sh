#!/bin/bash
#读取配置文件
command='PYSPARK_PYTHON=/opt/cloudera/parcels/Anaconda/bin/python /opt/spark-2.0.2/bin/spark-submit --queue root.users.gengqiling --master yarn-client --num-executors 40 --driver-cores 2 --driver-memory 10g --executor-memory 50g --executor-cores 10'
command+=" "$*
echo $command
eval $command