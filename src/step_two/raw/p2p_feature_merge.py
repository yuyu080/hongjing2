# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
p2p_feature_merge.py
'''
import os
import sys

import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "50g")
    conf.set("spark.executor.instances", 10)
    conf.set("spark.executor.cores", 10)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 1000)
    conf.set("spark.sql.shuffle.partitions", 1000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)
    
    spark = SparkSession \
        .builder \
        .appName("hgongjing2_two_raw_common_feature_merge") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark  

def spark_data_flow(static_version, dynamic_version, relation_version):
    '''
    合并静态、动态风险特征与p2p行业风险特征
    '''
    static_df = spark.read.json(        
        "{path}/"
        "common_static_feature_distribution/{version}".format(path=IN_PAHT, 
                                                              version=static_version))
    dynamic_df = spark.read.json(
        "{path}/"
        "common_dynamic_feature_distribution/{version}".format(path=IN_PAHT, 
                                                               version=dynamic_version))
    p2p_df = spark.read.json(
        "{path}/"
        "p2p_feature_distribution/{version}".format(path=IN_PAHT, 
                                                    version=relation_version))
    raw_df = p2p_df.join(
        static_df,
        static_df.company_name == p2p_df.company_name,
        'left_outer'
    ).join(
        dynamic_df,
        dynamic_df.company_name == p2p_df.company_name,
        'left_outer'
    ).select(
        p2p_df.bbd_qyxx_id
        ,p2p_df.company_name
        ,p2p_df.platform_name
        ,p2p_df.platform_state
        ,'p2p_feature_1'
        ,'p2p_feature_2'
        ,'p2p_feature_3'
        ,'p2p_feature_4'
        ,'p2p_feature_5'
        ,'p2p_feature_6'
        ,'p2p_feature_7'
        ,'p2p_feature_8'
        ,'p2p_feature_9'
        ,'p2p_feature_10'
        ,'p2p_feature_11'
        ,'p2p_feature_12'
        ,'p2p_feature_13'
        ,'p2p_feature_14'
        ,'p2p_feature_15'
        ,'p2p_feature_16'
        ,'p2p_feature_17'
        ,'p2p_feature_18'
        ,'p2p_feature_19'
        ,'p2p_feature_20'
        ,'p2p_feature_21'
        ,'feature_1'
        ,'feature_2'
        ,'feature_3'
        ,'feature_4'
        ,'feature_5'
        ,'feature_6'
        ,'feature_7'
        ,'feature_8'
        ,'feature_9'
        ,'feature_10'
        ,'feature_11'
        ,'feature_12'
        ,'feature_13'
        ,'feature_14'
        ,'feature_15'
        ,'feature_16'
        ,'feature_17'
        ,'feature_18'
        ,'feature_19'
        ,'feature_20'
        ,'feature_21'
        ,'feature_22'
        ,'feature_23'
        ,'feature_24'
        ,'feature_25'
        ,'feature_26'
        ,'feature_27'
        ,'feature_28'
    )
    
    return raw_df

def run():
    raw_df = spark_data_flow(static_version=RELATION_VERSION, 
                             dynamic_version=RELATION_VERSION,
                             relation_version=RELATION_VERSION)
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "p2p_feature_merge/{version}").format(path=OUT_PATH, 
                                               version=RELATION_VERSION))    
    raw_df.repartition(10).write.parquet(         
        ("{path}/"
         "p2p_feature_merge/{version}").format(path=OUT_PATH, 
                                               version=RELATION_VERSION))

if __name__ == '__main__':
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #输入参数
    RELATION_VERSION = sys.argv[1]
    
    IN_PAHT = conf.get('common_company_feature', 'OUT_PATH')
    OUT_PATH = conf.get('feature_merge', 'OUT_PATH')
    
    spark = get_spark_session()
    
    run()