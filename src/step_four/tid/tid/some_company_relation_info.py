# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 15g \
--queue project.hongjing \
some_company_relation_info.py {version}
'''

import sys
import os

from pyspark.sql import functions as fun
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "60g")
    conf.set("spark.executor.instances", 20)
    conf.set("spark.executor.cores", 15)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 2000)
    conf.set("spark.sql.shuffle.partitions", 2000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)
    
    spark = SparkSession \
        .builder \
        .appName("hgongjing2_all_info_merge") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark
    

def spark_data_flow(tidversion):
    tid_relation_df = spark.read.parquet(
        "{path}/"
        "all_info_merge"
        "/{version}".format(path=IN_PATH, 
                            version=RELATION_VERSION)
    )
    
    #数据预处理
    tmp_relation_1_df = tid_relation_df.select(
        'a',
        'b',
        'c',
        fun.when(
            tid_relation_df.b_is_black_company == 'black', 1
        ).otherwise(0).alias('b_is_black_company'),
        fun.when(
            tid_relation_df.c_is_black_company == 'black', 1
        ).otherwise(0).alias('c_is_black_company'),
        fun.when(
            tid_relation_df.b_is_high_company == True, 1
        ).otherwise(0).alias('b_is_high_company'),
        fun.when(
            tid_relation_df.c_is_high_company == True, 1
        ).otherwise(0).alias('c_is_high_company')
    ).cache(
    )
    
    #关联方节点去重
    tmp_relation_2_df = tmp_relation_1_df.select(
        'a',
        'b',
        'b_is_black_company',
        'b_is_high_company'
    ).union(
        tmp_relation_1_df.select(
            'a',
            'c',
            'c_is_black_company',
            'c_is_high_company'
        )
    ).dropDuplicates(
        ['a', 'b']
    ).groupBy(
        'a'
    ).agg(
        {'b_is_black_company': 'sum', 
         'b_is_high_company': 'sum'}
    ).withColumnRenamed(
        'sum(b_is_high_company)', 'high_relation_company_num'
    ).withColumnRenamed(
        'sum(b_is_black_company)', 'black_relation_company_num'
    ).cache(
    )
   
    return tmp_relation_2_df

    
def run():
    tid_df = spark_data_flow(RELATION_VERSION)

    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "som_company_relation_info"
         "/{version}").format(path=OUT_PATH, 
                              version=RELATION_VERSION))    
    tid_df.coalesce(
        30
    ).write.parquet(
        "{path}/"
        "som_company_relation_info/"
        "{version}".format(path=OUT_PATH, 
                           version=RELATION_VERSION)
    )


if __name__ == '__main__':  
    import configparser
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    #输入参数
    IN_PATH = "/user/antifraud/hongjing2/dataflow/step_four/tid/tid/"
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_four/tid/tid/"

    
    spark = get_spark_session()

    run()