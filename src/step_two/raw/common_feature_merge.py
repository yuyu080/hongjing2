# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
common_feature_merge.py
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

def spark_data_flow(static_version, dynamic_version):
    '''
    合并静态与动态风险：当动态风险不存时，其值取0
    '''
    static_df = spark.read.json(        
        "{path}/"
        "common_static_feature_distribution/{version}".format(path=IN_PAHT, 
                                                              version=static_version))
    dynamic_df = spark.read.json(
        "{path}/"
        "common_dynamic_feature_distribution/{version}".format(path=IN_PAHT, 
                                                               version=dynamic_version))

    feature_df = static_df.join(
        dynamic_df,
        static_df.company_name == dynamic_df.company_name,
        'left_outer'
    ).select(
        ['bbd_qyxx_id', 
         static_df.company_name,
         'feature_1', 'feature_10', 'feature_11', 'feature_12',
         'feature_13', 'feature_14', 'feature_15', 'feature_16',
         'feature_17', 'feature_18', 'feature_19','feature_2',
         'feature_20','feature_21', 'feature_22', 'feature_23',
         'feature_24','feature_3','feature_4','feature_5',
         'feature_6','feature_7','feature_8','feature_9',
         'feature_25', 'feature_26', 'feature_27', 'feature_28']
    ).fillna(0) \

    return feature_df
    
def run():
    raw_df = spark_data_flow(static_version=RAW_STATIC_VERSION, 
                             dynamic_version=RAW_DYNAMIC_VERSION)
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "common_feature_merge/{version}").format(path=OUT_PATH, 
                                                  version=RAW_VERSION))    
    raw_df.repartition(10).write.parquet(         
        ("{path}/"
         "common_feature_merge/{version}").format(path=OUT_PATH, 
                                                  version=RAW_VERSION))  
    
if __name__ == '__main__':
    #输入参数
    RAW_STATIC_VERSION, RAW_DYNAMIC_VERSION = ['20170403', '20170403']
    RAW_VERSION = '20170403'
    IN_PAHT = "/user/antifraud/hongjing2/dataflow/step_one/prd/"
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_two/raw/"
    
    spark = get_spark_session()
    
    run()