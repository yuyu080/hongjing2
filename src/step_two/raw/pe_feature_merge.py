# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
pe_feature_merge.py
'''

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import os


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
    pe_df = spark.read.json(
        "{path}/"
        "pe_feature_distribution/{version}".format(path=IN_PAHT, 
                                                   version=relation_version))

    raw_df = pe_df.join(
        static_df,
        static_df.company_name == pe_df.company_name,
        'left_outer'
    ).join(
        dynamic_df,
        dynamic_df.company_name == pe_df.company_name,
        'left_outer'
    ).select(
        pe_df.bbd_qyxx_id
        ,pe_df.company_name
        ,'pe_feature_1'
        ,'pe_feature_2'
        ,'pe_feature_3'
        ,'pe_feature_4'
        ,'pe_feature_5'
        ,'pe_feature_6'
        ,'pe_feature_7'
        ,'pe_feature_8'
        ,'pe_feature_9'
        ,'pe_feature_10'
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
    raw_df = spark_data_flow(static_version=RAW_STATIC_VERSION, 
                             dynamic_version=RAW_DYNAMIC_VERSION,
                             relation_version=RELATION_VERSION)
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "pe_feature_merge/{version}").format(path=OUT_PATH, 
                                              version=RELATION_VERSION))    
    raw_df.repartition(10).write.parquet(         
        ("{path}/"
         "pe_feature_merge/{version}").format(path=OUT_PATH, 
                                              version=RELATION_VERSION))

if __name__ == '__main__':
    #输入参数
    RAW_STATIC_VERSION, RAW_DYNAMIC_VERSION = ['20170403', '20170403']
    #中间结果版本
    RELATION_VERSION = '20170403' 
    
    IN_PAHT = "/user/antifraud/hongjing2/dataflow/step_one/prd/"
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_two/raw/"
    
    spark = get_spark_session()
    
    run()