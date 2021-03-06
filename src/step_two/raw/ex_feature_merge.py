# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
ex_feature_merge.py {version}
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
        .appName("hgongjing2_two_raw_ex_feature_merge") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark  
    
def spark_data_flow(static_version, dynamic_version, relation_version):
    '''
    合并静态、动态风险特征与交易平台风险特征
    '''
    static_df = spark.read.json(        
        "{path}/"
        "common_static_feature_distribution_v2/"
        "{version}".format(path=IN_PATH_ONE, 
                           version=static_version))
    dynamic_df = spark.read.json(
        "{path}/"
        "common_dynamic_feature_distribution_v2/"
        "{version}".format(path=IN_PATH_ONE, 
                           version=dynamic_version))
    ex_df = spark.read.json(
        "{path}/"
        "ex_feature_distribution/"
        "{version}".format(path=IN_PATH_ONE, 
                           version=relation_version))
    
    sample_df = spark.read.parquet(
         ("{path}"
          "/ljr_sample/"
          "{version}").format(path=IN_PATH_FOUR, 
                              version=RELATION_VERSION))       
    
    raw_df = sample_df.where(
        sample_df.company_type == u'交易场所'    
    ).join(
        ex_df,
        ex_df.bbd_qyxx_id == sample_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        static_df,
        static_df.bbd_qyxx_id == sample_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        dynamic_df,
        dynamic_df.bbd_qyxx_id == sample_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        sample_df.bbd_qyxx_id
        ,sample_df.company_name
        ,'ex_feature_1'
        ,'ex_feature_2'
        ,'ex_feature_3'
        ,'ex_feature_4'
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
    ).fillna(
        0.    
    ).fillna(
        "-"
    )
    
    return raw_df

def run():
    raw_df = spark_data_flow(static_version=RELATION_VERSION, 
                             dynamic_version=RELATION_VERSION,
                             relation_version=RELATION_VERSION)
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "ex_feature_merge/{version}").format(path=OUT_PATH, 
                                               version=RELATION_VERSION))    
    raw_df.repartition(10).write.parquet(         
        ("{path}/"
         "ex_feature_merge/{version}").format(path=OUT_PATH, 
                                               version=RELATION_VERSION))

if __name__ == '__main__':
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #输入参数
    #交易平台日期
    EX_MEMBER_VERSION = conf.get('ex_feature_merge', 'EX_MEMBER_VERSION')
    RELATION_VERSION = sys.argv[1]
    
    IN_PATH_ONE = conf.get('common_company_feature', 'OUT_PATH')
    IN_PATH_TWO = conf.get('common_company_info', 'OUT_PATH')
    IN_PATH_THREE = conf.get('risk_score', 'OUT_PATH')
    IN_PATH_FOUR = conf.get('input_sample_data', 'OUT_PATH')
    OUT_PATH = conf.get('feature_merge', 'OUT_PATH')
    
    spark = get_spark_session()
    
    run()
    