# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
nf_info_merge.py {version}
'''
import sys
import os
import json

import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.sql import Row

def get_json_obj(row):
    '''将一级指标合在一起'''
    
    name_mapping = {
        'GM_behavior_risk': u'经营行为风险',
        'GM_company_strength_risk': u'综合实力风险',
        'GM_credit_risk': u'企业诚信风险',
        'GM_static_relationship_risk': u'静态关联方风险',
        'GM_dynamic_relationship_risk': u'动态关联方风险',
    }
    
    risk_composition = {'--': {
        name_mapping[k]: v 
        for k,v in row.iteritems() 
        if k not in ['bbd_qyxx_id', 'total_score',
                     'company_name']}}
    risk_composition['--']['total_score'] = row['total_score']
    
    return Row(
        bbd_qyxx_id=row['bbd_qyxx_id'],
        company_name=row['company_name'],
        risk_index=row['total_score'],
        risk_composition=json.dumps(
            risk_composition,
            ensure_ascii=False)
    )

def get_black(col):
    return True

def spark_data_flow():
    '''
    计算新金融部分的信息，
    1、将一级指标合弄成一个json_obj
    2、计算区域分布
    3、目前企业是否是黑企业
    '''
    get_black_udf = fun.udf(get_black, tp.BooleanType())
    
    raw_basic_df = spark.read.parquet(
        ("{path}/basic/{version}").format(path=IN_PATH_ONE,
                                          version=RELATION_VERSION))
    raw_nf_risk_score_df = spark.read.json(
        ("{path}/"
         "/nf_feature_risk_score"
         "/{version}").format(path=IN_PATH_TWO,
                              version=RELATION_VERSION))
    county_mapping_df = spark.read.csv(
        "{path}".format(path=MAPPING_PATH),
        sep='\t', 
        header=True)
    black_df = spark.read.parquet(
        ("{path}/black_company/{version}").format(path=IN_PATH_ONE,
                                                  version=RELATION_VERSION)
    ).withColumn(
        'is_black', get_black_udf('company_name')
    )
    
    tid_nf_risk_score_df = raw_nf_risk_score_df.rdd.map(
        lambda r: r.asDict()
    ).map(
        get_json_obj
    ).toDF(
    )
    tid_nf_risk_score_df = tid_nf_risk_score_df.join(
        raw_basic_df,
        raw_basic_df.bbd_qyxx_id == tid_nf_risk_score_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_nf_risk_score_df.bbd_qyxx_id,
        tid_nf_risk_score_df.company_name,
        tid_nf_risk_score_df.risk_index,
        tid_nf_risk_score_df.risk_composition,
        raw_basic_df.company_county
    )
    tid_nf_risk_score_df = tid_nf_risk_score_df.join(
        county_mapping_df,
        county_mapping_df.code == tid_nf_risk_score_df.company_county,
        'left_outer'
    ).join(
        black_df,
        black_df.bbd_qyxx_id == tid_nf_risk_score_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_nf_risk_score_df.bbd_qyxx_id,
        tid_nf_risk_score_df.company_name,
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
    return tid_nf_risk_score_df

def run():
    raw_df = spark_data_flow()
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "nf_info_merge/{version}").format(path=OUT_PATH, 
                                           version=RELATION_VERSION))    
    raw_df.repartition(10).write.parquet(         
        ("{path}/"
         "nf_info_merge/{version}").format(path=OUT_PATH, 
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
        .appName("hgongjing2_three_raw_nf_info") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark    

if __name__ == '__main__':
    conf = configparser.ConfigParser()
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")

    #中间结果版本
    RELATION_VERSION = sys.argv[1]

    IN_PATH_ONE = conf.get('common_company_info', 'OUT_PATH')
    IN_PATH_TWO = conf.get('risk_score', 'OUT_PATH')    
    OUT_PATH = conf.get('info_merge', 'OUT_PATH')
    MAPPING_PATH = conf.get('info_merge', 'MAPPING_PATH')    
    
    spark = get_spark_session()
    
    run()