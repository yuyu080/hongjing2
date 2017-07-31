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
        {table}      
        WHERE
        dt='{version}'
        '''.format(table=TABLE_NAME,
                   version=TAGS_VERSION)
    )
    tid_tags_df = raw_tags_df.where(
        raw_tags_df.company_type.isin(TYPE_LIST)
    ).withColumn(
        'weight', get_type_weight_udf('company_type')
    )
    
    #企业白名单
    raw_white_tasg_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        tag company_type
        FROM
        {table} 
        WHERE
        dt='{version}'
        '''.format(table=WHITE_TABLE_NAME,
                   version=WHITE_TAGS_VERSION)
    )
    tid_white_tasg_df = raw_white_tasg_df.where(
        raw_white_tasg_df.company_type.isin(TYPE_LIST)
    ).withColumn(
        'weight', get_type_weight_udf('company_type')
    )
    
    #合并原始数据与白名单数据
    raw_all_df = tid_tags_df.union(
        tid_white_tasg_df
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
    
    #过滤一部分企业
    filter_df = spark.sql(
        '''
        SELECT
        company_name,
        bbd_qyxx_id,
        tag
        FROM
        {table}
        WHERE
        dt='{version}'
        '''.format(table=BLACK_TABLE_NAME,
                   version=BLACK_VERSION)
    )
    #这里有个特殊规则：通过过滤名单中的“新兴金融”类企业过滤除私募基金外的所有企业
    hongjing_filter_df = filter_df.where(
        filter_df.tag.isin(TYPE_LIST)
shi     )
    tid_all_df = tid_all_df.join(
        hongjing_filter_df,
        hongjing_filter_df.bbd_qyxx_id == tid_all_df.bbd_qyxx_id, 
        'left_outer'
    ).where(
        fun.when(
            hongjing_filter_df.bbd_qyxx_id.isNull(), True
        ).when(
            tid_all_df.company_type == u'私募基金', True
        )
    ).select(
        tid_all_df.bbd_qyxx_id,
        tid_all_df.company_name,
        tid_all_df.company_type
    ).cache()
    
    #输出
    prd_all_df = tid_all_df.dropDuplicates(
        ['bbd_qyxx_id', 'company_name']
    ).select(
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
    #qyxx_tag
    TABLE_NAME = conf.get('input_sample_data', 'TABLE_NAME')
    TAGS_VERSION = conf.get('input_sample_data', 'TAGS_VERSION')
    TYPE_LIST = eval(conf.get('input_sample_data', 'TYPE_LIST'))

    #需要增加的白名单
    WHITE_TABLE_NAME = conf.get('input_sample_data', 'WHITE_TABLE_NAME')
    WHITE_TAGS_VERSION = eval(conf.get('input_sample_data', 
                                       'WHITE_TAGS_VERSION'))
    
    #需要过滤的数据版本
    BLACK_TABLE_NAME = conf.get('input_sample_data', 'BLACK_TABLE_NAME')
    BLACK_VERSION = conf.get('input_sample_data', 'BLACK_VERSION')
    
    #中间结果版本，输出版本
    RELATION_VERSION = sys.argv[1]
    
    OUT_PATH = conf.get('input_sample_data', 'OUT_PATH')
    
    spark = get_spark_session()
    
    run()