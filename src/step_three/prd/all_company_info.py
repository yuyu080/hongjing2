# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
all_company_info.py
'''

import os
import json
import random

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.sql import Row

def get_nf_type(col):
    return u'新兴金融'
    
def get_p2p_type(col):
    return u'网络借贷'
    
def get_pe_type(col):
    return u'私募基金'
    
def get_ex_type(col):
    return u'交易场所'

def get_risk_rank():
    '''
    临时生成危险等级
    '''
    risk_degree = random.randint(1, 3)
    risk_dict = {
        1: u'高危预警',
        2: u'重点关注',
        3: u'持续监控'
    }
    return risk_dict[risk_degree]

def get_json_obj(row):
    json_obj = json.dumps(
        {u'工商变更': row['gsbg'],
         u'诉讼信息': row['ssxx'],
         u'行政处罚': row['xzcf'],
         u'失信信息': row['sxxx'],
         u'经营异常': row['jyyc'],
         u'分支机构': row['fzjg']},
         
        ensure_ascii=False
    )
    return Row(
        bbd_qyxx_id=row['bbd_qyxx_id'],
        company_name=row['company_name'],
        xgxx_info=json_obj
    )

def get_data_version():
    return RELATION_VERSION

def raw_spark_data_flow():
    '''
    合并前各行业企业的所有信息，并输出单一时间点的最终结果
    '''
    get_nf_type_udf = fun.udf(get_nf_type, tp.StringType())
    get_p2p_type_udf = fun.udf(get_p2p_type, tp.StringType())
    get_pe_type_udf = fun.udf(get_pe_type, tp.StringType())
    get_ex_type_udf = fun.udf(get_ex_type, tp.StringType())
    
    raw_nf_df = spark.read.parquet(
            ("/user/antifraud/hongjing2/dataflow/step_three/tid"
            "/nf_feature_tags/{version}").format(version='20170403')) \
        .withColumn('company_type', get_nf_type_udf('company_name'))
    
    p2p_df = spark.read.parquet(
            ("/user/antifraud/hongjing2/dataflow/step_three/tid"
            "/p2p_feature_tags/{version}").format(version='20170403')) \
        .withColumn('company_type', get_p2p_type_udf('company_name'))
    
    pe_df = spark.read.parquet(
            ("/user/antifraud/hongjing2/dataflow/step_three/tid"
            "/pe_feature_tags/{version}").format(version='20170403')) \
        .withColumn('company_type', get_pe_type_udf('company_name'))
    
    ex_df = spark.read.parquet(
            ("/user/antifraud/hongjing2/dataflow/step_three/tid"
            "/ex_feature_tags/{version}").format(version='20170403')) \
        .withColumn('company_type', get_ex_type_udf('company_name'))
 
    #确保一个公司只会存在一条记录
    industry_df = p2p_df.union(
        pe_df
    ).union(
        ex_df
    ).dropDuplicates(['company_name'])
    tmp_nf_df = raw_nf_df.join(
        industry_df,
        industry_df.company_name == raw_nf_df.company_name,
        'left_outer'
    ).where(
        industry_df.company_name.isNull()
    ).select(
        raw_nf_df.company_name,
        raw_nf_df.bbd_qyxx_id,
        raw_nf_df.risk_tags,
        raw_nf_df.risk_index,
        raw_nf_df.risk_composition,
        raw_nf_df.province,
        raw_nf_df.city,
        raw_nf_df.county,
        raw_nf_df.is_black,
        raw_nf_df.company_type
    )
    tid_nf_df = tmp_nf_df.union(industry_df)    
    return tid_nf_df    

def tid_spark_data_flow():
    '''
    获取某目标公司的相关信息
    '''
    common_static_feature_df = spark.read.json(
        ("/user/antifraud/hongjing2/dataflow/step_one/prd"
         "/common_static_feature_distribution"
         "/{version}").format(version=RELATION_VERSION))    
    raw_xgxx_info = common_static_feature_df.select(
        'bbd_qyxx_id',
        'company_name',
        (common_static_feature_df.feature_6.getItem('c_1') + 
        common_static_feature_df.feature_6.getItem('c_2') +
        common_static_feature_df.feature_6.getItem('c_3') +
        common_static_feature_df.feature_6.getItem('c_4') +
        common_static_feature_df.feature_6.getItem('c_5')
        ).alias('gsbg'),
        (common_static_feature_df.feature_10.getItem('0').getItem('ktgg') +
        common_static_feature_df.feature_10.getItem('0').getItem('rmfygg') +
        common_static_feature_df.feature_10.getItem('0').getItem('zgcpwsw')
        ).alias('ssxx'),
        (common_static_feature_df.feature_11.getItem('0').getItem('xzcf') +
        common_static_feature_df.feature_14.getItem('0').getItem('circxzcf')
        ).alias('xzcf'),
        (common_static_feature_df.feature_12.getItem('0').getItem('dishonesty') +
        common_static_feature_df.feature_12.getItem('0').getItem('zhixing')
        ).alias('sxxx'),
        (common_static_feature_df.feature_13.getItem('0').getItem('jyyc')
        ).alias('jyyc'),
        (common_static_feature_df.feature_18.getItem('d')
        ).alias('fzjg')
    ).rdd.map(
        get_json_obj
    ).toDF()
    return raw_xgxx_info
        
def spark_data_flow():
    '''
    输出一个版本的整合数据
    '''
    #输入
    tid_nf_df = raw_spark_data_flow()
    raw_xgxx_info = tid_spark_data_flow()
    #输出
    get_risk_rank_udf = fun.udf(get_risk_rank, tp.StringType())
    get_data_version_udf = fun.udf(get_data_version, tp.StringType())
    prd_nf_df = tid_nf_df.join(
        raw_xgxx_info,
        raw_xgxx_info.company_name == tid_nf_df.company_name,
        'left_outer'
    ).select(
        tid_nf_df.company_name,
        tid_nf_df.bbd_qyxx_id,
        tid_nf_df.risk_tags,
        tid_nf_df.risk_index,
        tid_nf_df.risk_composition,
        tid_nf_df.province,
        tid_nf_df.city,
        tid_nf_df.county,
        tid_nf_df.is_black,
        tid_nf_df.company_type,
        raw_xgxx_info.xgxx_info,
        get_risk_rank_udf().alias('risk_rank'),
        get_data_version_udf().alias('data_version'),
        fun.current_timestamp().alias('gmt_create'),
        fun.current_timestamp().alias('gmt_update')
    )
    return prd_nf_df

def run():
    prd_df = spark_data_flow()
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "all_company_info/{version}").format(path=OUT_PATH, 
                                           version=RELATION_VERSION))    
    prd_df.repartition(10).write.parquet(         
        ("{path}/"
         "all_company_info/{version}").format(path=OUT_PATH, 
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
        .appName("hgongjing2_three_prd_all_info") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 

if __name__ == '__main__':
    #中间结果版本
    RELATION_VERSION = '20170117' 
    
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_three/prd/"
    
    spark = get_spark_session()
    
    run()