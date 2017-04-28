# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
ex_info_merge.py
'''

import os
import json

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.sql import Row

def get_json_obj(row):
    '''将一级指标合在一起'''
    return Row(
        bbd_qyxx_id=row['bbd_qyxx_id'],
        company_name=row['company_name'],
        risk_index=row['total_score'],
        risk_composition=json.dumps(
            {k: v for k,v in row.iteritems() 
                 if k not in ['bbd_qyxx_id', 'total_score', 'company_name']})
    )

def get_black(col):
    return True

def spark_data_flow():
    '''
    计算通用部分的信息，
    1、将一级指标合弄成一个json_obj
    2、计算区域分布
    3、目前企业是否是黑企业
    '''
    get_black_udf = fun.udf(get_black, tp.BooleanType())
    
    raw_basic_df = spark.read.parquet(
        ("/user/antifraud/hongjing2/dataflow/step_one/raw"
         "/basic/{version}").format(version=RELATION_VERSION))
    raw_ex_risk_score_df = spark.read.json(
        ("/user/antifraud/hongjing2/dataflow/step_two/prd"
         "/ex_feature_risk_score"
         "/{version}").format(version=RELATION_VERSION))
    county_mapping_df = spark.read.csv(
        "/user/antifraud/source/company_county_mapping", 
        sep='\t', 
        header=True)
    black_df = spark.sql(
        '''
        SELECT 
        company_name 
        FROM 
        dw.qyxg_leijinrong_blacklist
        '''
    ).distinct(
    ).withColumn('is_black', get_black_udf('company_name'))
    
    tid_ex_risk_score_df = raw_ex_risk_score_df.rdd.map(
        lambda r: r.asDict()
    ).map(
        get_json_obj
    ).toDF(
    )
    tid_ex_risk_score_df = tid_ex_risk_score_df.join(
        raw_basic_df,
        raw_basic_df.bbd_qyxx_id == tid_ex_risk_score_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_ex_risk_score_df.bbd_qyxx_id,
        tid_ex_risk_score_df.company_name,
        tid_ex_risk_score_df.risk_index,
        tid_ex_risk_score_df.risk_composition,
        raw_basic_df.company_county
    )
    tid_ex_risk_score_df = tid_ex_risk_score_df.join(
        county_mapping_df,
        county_mapping_df.code == tid_ex_risk_score_df.company_county,
        'left_outer'
    ).join(
        black_df,
        black_df.company_name == tid_ex_risk_score_df.company_name,
        'left_outer'
    ).select(
        'bbd_qyxx_id',
        tid_ex_risk_score_df.company_name,
        'risk_index',
        'risk_composition',
        'company_county',
        'province',
        'city',
        'county',
        'is_black'
    ).dropDuplicates(
        ['company_name']
    )
    
    return tid_ex_risk_score_df

def run():
    raw_df = spark_data_flow()
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "ex_info_merge/{version}").format(path=OUT_PATH, 
                                           version=RELATION_VERSION))    
    raw_df.repartition(10).write.parquet(         
        ("{path}/"
         "ex_info_merge/{version}").format(path=OUT_PATH, 
                                           version=RELATION_VERSION))

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
        .appName("hgongjing2_three_raw_ex_info") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark
    
if __name__ == '__main__':
    #中间结果版本
    RELATION_VERSION = '20170117' 
    
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_three/raw/"
    
    spark = get_spark_session()
    
    run()