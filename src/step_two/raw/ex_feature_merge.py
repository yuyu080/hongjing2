# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
ex_feature_merge.py {version}
'''
import os
import sys

import configparser
from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def is_black(col):
    return 1

def is_risk(col):
    return 1    

def get_ex_feature_3(col):
    '''
    违规会员单位风险
    '''
    if col == 0:
        risk = 10.
    elif 0 < col <= 3:
        risk = 50.
    else:
        risk = 100.
    return risk

def get_ex_feature_4(col):
    '''
    高风险会员单位风险
    '''
    if col == 0:
        risk = 10.
    elif 0 < col <= 3:
        risk = 30.
    elif 3 < col <= 5:
        risk = 60.
    else:
        risk = 100.
    return risk
    
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
    
def ex_supplement_flow():
    '''
    交易平台独有特征，需要依赖新金融的数据,用于计算会员企业风险
    '''    
    #黑企业名单: dw.qyxg_leijinrong_blacklist
    is_black_udf = fun.udf(is_black, tp.IntegerType())
    nf_black_df = spark.sql(
        '''
        SELECT
        company_name
        FROM 
        dw.qyxg_leijinrong_blacklist
        '''
    ).dropDuplicates(
        ['company_name']
    ).withColumn(
        'is_black', is_black_udf('company_name')
    )
    
    #取新金融top5%的企业
    is_risk_udf = fun.udf(is_risk, tp.IntegerType())
    raw_nf_top_df = spark.read.json(
        ("/user/antifraud/hongjing2/dataflow/step_two/prd"
         "/nf_feature_risk_score"
         "/{version}").format(version=RELATION_VERSION)
    ).select(
        'company_name',
        'total_score'
    ).cache()
    top_num = int(raw_nf_top_df.count() * 0.05)
    tid_nf_top_df = raw_nf_top_df \
        .sort(raw_nf_top_df.total_score.desc()) \
        .limit(top_num)
    prd_nf_top_df = tid_nf_top_df.withColumn(
        'is_risk', is_risk_udf('company_name')
    ).dropDuplicates(
        ['company_name']
    )
    
    #会员名单: dw.qyxg_ex_member_list
    raw_ex_member_df = spark.sql(
        '''
        SELECT
        exchange_name,
        company_name
        FROM
        dw.qyxg_ex_member_list
        WHERE
        dt='{version}'
        '''.format(version=EX_MEMBER_VERSION)
    ).dropDuplicates(
        ['exchange_name', 'company_name']
    )
    
    tid_ex_member_df = raw_ex_member_df.join(
        nf_black_df,
        nf_black_df.company_name == raw_ex_member_df.company_name,
        'left_outer'
    ).join(
        prd_nf_top_df,
        prd_nf_top_df.company_name == raw_ex_member_df.company_name,
        'left_outer'
    ).select(
        raw_ex_member_df.exchange_name,
        raw_ex_member_df.company_name,
        nf_black_df.is_black,
        prd_nf_top_df.is_risk
    )
    
    prd_ex_member_df = tid_ex_member_df \
        .fillna(0) \
        .groupBy('exchange_name') \
        .agg({'is_black': 'sum', 'is_risk': 'sum'}) \
        .withColumnRenamed('sum(is_black)', 'black_num') \
        .withColumnRenamed('sum(is_risk)', 'risk_num')
        
    get_ex_feature_3_udf = fun.udf(get_ex_feature_3, tp.DoubleType())
    get_ex_feature_4_udf = fun.udf(get_ex_feature_4, tp.DoubleType())
    
    prd_ex_member_df = prd_ex_member_df.select(
        prd_ex_member_df.exchange_name.alias('company_name'),
        'black_num',
        'risk_num',
        get_ex_feature_3_udf('black_num').alias('ex_feature_3'),
        get_ex_feature_4_udf('risk_num').alias('ex_feature_4')
    )
    
    return prd_ex_member_df
    
    
def spark_data_flow(static_version, dynamic_version, relation_version):
    '''
    合并静态、动态风险特征与交易平台风险特征
    '''
    static_df = spark.read.json(        
        "{path}/"
        "common_static_feature_distribution/{version}".format(path=IN_PAHT, 
                                                              version=static_version))
    dynamic_df = spark.read.json(
        "{path}/"
        "common_dynamic_feature_distribution/{version}".format(path=IN_PAHT, 
                                                               version=dynamic_version))
    ex_df = spark.read.json(
        "{path}/"
        "ex_feature_distribution/{version}".format(path=IN_PAHT, 
                                                   version=relation_version))
    #交易平台的补充指标
    ex_supplement_df = ex_supplement_flow()
    
    raw_df = ex_df.join(
        static_df,
        static_df.company_name == ex_df.company_name,
        'left_outer'
    ).join(
        dynamic_df,
        dynamic_df.company_name == ex_df.company_name,
        'left_outer'
    ).join(
        ex_supplement_df,
        ex_supplement_df.company_name == ex_df.company_name,
        'left_outer'
    ).select(
        ex_df.bbd_qyxx_id
        ,ex_df.company_name
        ,'ex_feature_1'
        ,'ex_feature_2'
        ,fun.when(
            ex_supplement_df.ex_feature_3.isNull(), 0
        ).otherwise(
            ex_supplement_df.ex_feature_3
        ).alias('ex_feature_3')
        ,fun.when(
            ex_supplement_df.ex_feature_4.isNull(), 0
        ).otherwise(
            ex_supplement_df.ex_feature_4
        ).alias('ex_feature_4')
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
    
    IN_PAHT = conf.get('ex_feature_merge', 'IN_PAHT')
    OUT_PATH = conf.get('ex_feature_merge', 'OUT_PATH')
    
    spark = get_spark_session()
    
    run()
    