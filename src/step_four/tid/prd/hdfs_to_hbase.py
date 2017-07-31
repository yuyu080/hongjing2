# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 15g \
hdfs_to_hbase.py {version}
'''

import sys
import os
import json
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def get_all_feature(row):
    try:
        row_dict = row.asDict()
        return [u'\t'.join([row_dict['bbd_qyxx_id'], 
                            k, 
                            json.dumps(v, 
                                       ensure_ascii=False)]) 
                for k, v in row_dict.iteritems()]
    except:
        return []

def get_spark_session():
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "20g")
    conf.set("spark.executor.instances", 30)
    conf.set("spark.executor.cores", 5)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 1000)
    conf.set("spark.sql.shuffle.partitions", 1000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)

    spark = SparkSession \
        .builder \
        .appName("hgongjing2_hdfs_to_hbase") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark
    
def spark_data_flow():    
    prd_df = spark.read.json(
        ("{path}/"
         "all_company_feature/"
         "{version}").format(path=IN_PATH,
                             version=RELATION_VERSION)
    ).rdd.flatMap(
        get_all_feature
    ).coalesce(
        500
    )
    
    return prd_df

def run():
    tid_df = spark_data_flow()
    
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "hdfs_to_hbase"
         "/{version}").format(path=OUT_PATH, 
                              version=RELATION_VERSION))
    
    tid_df.saveAsTextFile(
        ("{path}/"
         "hdfs_to_hbase"
         "/{version}").format(path=OUT_PATH,
                              version=RELATION_VERSION),
        compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

if __name__ == '__main__':
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    #输入路径
    IN_PATH = "/user/antifraud/hongjing2/dataflow/step_four/tid/prd/"
    #输出路径
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_four/tid/prd/"
    
    spark = get_spark_session()
    
    run()