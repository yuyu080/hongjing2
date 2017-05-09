# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
input_sample_data.py {version}
'''
import os
import sys

import configparser
from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def get_type_weight(company_type):
    '''根据公司所属的行业赋权，为排序做准备'''
    return WEIGHT_DICT[company_type]

def raw_spark_data_flow():
    '''
    这里的关键是根据bbd_qyxx_id与company_name去重，
    这是为了保证去重的同时而不过滤掉那些没有id的企业
    '''
    get_type_weight_udf = fun.udf(get_type_weight, tp.IntegerType())
    #适用于通用模型的部分
    raw_tags_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        company_name,
        tag company_type
        FROM 
        dw.qyxx_tags 
        WHERE
        dt='{version}'
        AND
        bbd_status = 1
        '''.format(version=TAGS_VERSION)
    )
    tid_tags_df = raw_tags_df.where(
        raw_tags_df.company_type.isin(TYPE_LIST)
    ).withColumn(
        'weight', get_type_weight_udf('company_type')
    ).dropDuplicates(
        ['bbd_qyxx_id', 'company_name']
    )
    
    #网络借贷P2P 
    raw_wdzj_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        '网络借贷' company_type
        FROM
        dw.qyxg_wdzj
        WHERE
        dt='{version}'
        '''.format(version=WDZJ_VERSION)
    )
    
    platform_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        '网络借贷' company_type
        FROM
        dw.qyxg_platform_data
        WHERE
        dt = '{version}'
        '''.format(
            version=PLATFORM_VERSION
        )
    )
    
    tid_wljd_df = raw_wdzj_df.union(
        platform_df
    ).withColumn(
        'weight', get_type_weight_udf('company_type')
    ).dropDuplicates(
        ['bbd_qyxx_id', 'company_name']
    )
    
    #私募基金
    smjj_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        fund_manager_chinese company_name,
        '私募基金' company_type
        FROM
        dw.qyxg_jijin_simu
        WHERE
        dt = '{version}'
        '''.format(
            version=SMJJ_VERSION
        )
    ).withColumn(
        'weight', get_type_weight_udf('company_type')
    ).dropDuplicates(
        ['bbd_qyxx_id', 'company_name']
    )
    
    #交易场所
    exchange_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        '交易场所' company_type
        FROM
        dw.qyxg_exchange
        WHERE
        dt='{version}'
        '''.format(version=EXCHANGE_VERSION)
    ).withColumn(
        'weight', get_type_weight_udf('company_type')
    ).dropDuplicates(
        ['bbd_qyxx_id', 'company_name']
    )
    
    #合并各个行业的数据
    raw_all_df = tid_tags_df.union(
        tid_wljd_df
    ).union(
        smjj_df
    ).union(
        exchange_df
    )
    
    return raw_all_df

def spark_data_flow():
    raw_all_df = raw_spark_data_flow()    
    
    #取权重最高的那一个company_type
    window = Window.partitionBy(
        ['bbd_qyxx_id']
    ).orderBy(
        raw_all_df.weight.desc()
    )
    tid_all_df = raw_all_df.select(
        'bbd_qyxx_id',
        'company_name',
        'company_type',
        'weight',
        row_number().over(window).alias('row_number')
    ).where(
        'row_number == 1'
    )
    
    #输出
    prd_all_df = tid_all_df.select(
        'bbd_qyxx_id',
        'company_name',
        'company_type',    
    )    
    
    return prd_all_df

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
        .appName("hgongjing2_two_zero_input_sample_data") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark  

def run():
    raw_df = spark_data_flow()
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "ljr_sample/{version}").format(path=OUT_PATH, 
                                        version=RELATION_VERSION))    
    raw_df.repartition(10).write.parquet(         
        ("{path}/"
         "ljr_sample/{version}").format(path=OUT_PATH, 
                                        version=RELATION_VERSION))

if __name__ == '__main__':
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    #输入参数
    
    #权重分布，值越大越重要
    WEIGHT_DICT = eval(conf.get('input_sample_data', 'WEIGHT_DICT'))
    
    #适用于通用模型的部分
    #dw.qyxx_tags 
    TAGS_VERSION = conf.get('input_sample_data', 'TAGS_VERSION')
    TYPE_LIST = eval(conf.get('input_sample_data', 'TYPE_LIST'))
    
    #网络借贷P2P
    #dw.qyxg_platform_data
    #dw.qyxg_wdzj
    PLATFORM_VERSION = conf.get('input_sample_data', 'PLATFORM_VERSION')
    WDZJ_VERSION = conf.get('input_sample_data', 'WDZJ_VERSION')
    
    #私募基金
    #dw.qyxg_jijin_simu
    SMJJ_VERSION = conf.get('input_sample_data', 'SMJJ_VERSION')

    #交易场所
    #dw.qyxg_exchange
    EXCHANGE_VERSION = conf.get('input_sample_data', 'EXCHANGE_VERSION')
    
    #中间结果版本，输出版本
    RELATION_VERSION = sys.argv[1]
    
    OUT_PATH = conf.get('input_sample_data', 'OUT_PATH')
    
    spark = get_spark_session()
    
    run()