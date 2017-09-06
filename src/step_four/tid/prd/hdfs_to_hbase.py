# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 15g \
--queue project.hongjing \
hdfs_to_hbase.py {version}
'''

import sys
import os
import json
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def get_spark_session():
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "40g")
    conf.set("spark.executor.instances", 30)
    conf.set("spark.executor.cores", 8)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 2000)
    conf.set("spark.sql.shuffle.partitions", 2000)
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
    raw_rdd = spark.sparkContext.textFile(
        ("{path}/"
         "all_company_feature/"
         "{version}").format(path=IN_PATH,
                             version=RELATION_VERSION)
    ).map(
        json.loads
    ).cache()

    data = raw_rdd.take(1)
    columns = sorted(data[0].keys())
        
    def get_feature(row):
        row_key = row['bbd_qyxx_id']
        row_data = [json.dumps(row[k], ensure_ascii=False) 
                 if k != 'bbd_qyxx_id' 
                 and k != 'company_name' 
                 else row[k]
            for k in columns]
        row_data.insert(0, row_key)
        
        return '\t'.join(row_data)
         
    prd_rdd = raw_rdd.map(
        get_feature
    ).coalesce(
        500
    )
    
    return prd_rdd

def run():
    prd_rdd = spark_data_flow()
    
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "hdfs_to_hbase"
         "/{version}").format(path=OUT_PATH, 
                              version=RELATION_VERSION))
    
    prd_rdd.saveAsTextFile(
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