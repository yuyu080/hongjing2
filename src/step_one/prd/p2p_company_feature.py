# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
p2p_company_feature.py {version}

'''
import sys
import json
import re
import os

import configparser
from pyspark.sql import types as tp
from pyspark.sql import functions as fun
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

def get_p2p_feature_19(col):
    '''
    自动投标风险
    '''
    if u'不支持' in col:
        risk = 0.
    elif u'支持' in col:
        risk = 100.
    else:
        risk = 0.
    return risk

def get_p2p_feature_20(col):
    '''
    债权转让
    '''
    if u'不可转让' in col or u'-' in col:
        risk = 0.
    else:
        risk = 100.
    return risk

def get_p2p_feature_21(col):
    '''
    资金托管
    '''
    if u'无存管' in col:
        risk = 100.
    else:
        risk = 0.
    return risk

def get_float(value):
    try:
        return round(float(re.search('[\d\.\,]+', 
                                       value).group().replace(',', '')), 2)
    except:
        return 0.

def json_to_obj(col):
    obj = json.loads(col)[0]
    obj = {k: get_float(v) for k, v in obj.iteritems()}
    return obj

def get_unique_string(col1, col2):
    '''
    在outer join后，返回唯一的一个值
    '''
    if col1:
        return col1
    else:
        return col2

def spark_data_flow():
    json_to_obj_udf = fun.udf(json_to_obj, 
                              tp.MapType(tp.StringType(), tp.FloatType()))    
    get_float_udf = fun.udf(get_float, tp.FloatType())
    get_p2p_feature_19_udf = fun.udf(get_p2p_feature_19, tp.DoubleType())
    get_p2p_feature_20_udf = fun.udf(get_p2p_feature_20, tp.DoubleType())
    get_p2p_feature_21_udf = fun.udf(get_p2p_feature_21, tp.DoubleType())
    get_unique_string_udf = fun.udf(get_unique_string, tp.StringType())    
    
    raw_wdzj_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        platform_name,
        automatic_bidding,
        claim_transfer,
        bank_custody,
        platform_state
        FROM
        dw.qyxg_wdzj
        WHERE
        dt='{version}'
        '''.format(version=WDZJ_VERSION)
    )
    tid_wdzj_df = raw_wdzj_df.select(
        'bbd_qyxx_id',
        'company_name',
        'platform_name',
        'platform_state',
        get_p2p_feature_19_udf('automatic_bidding').alias('p2p_feature_19'),
        get_p2p_feature_20_udf('claim_transfer').alias('p2p_feature_20'),
        get_p2p_feature_21_udf('bank_custody').alias('p2p_feature_21')
    )
    
    platform_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id
        ,company_name
        ,platform_name
        ,per_lending_amount
        ,avg_soldout_time 
        ,total_num_of_lender 
        ,total_turnover 
        ,total_deal_volume 
        ,monthly_deal_data 
        ,per_lending_num 
        ,avg_lend_time 
        ,per_borrowing_num 
        ,total_num_of_borrower 
        ,lending_dispersion 
        ,loan_balance 
        ,per_borrowing_amount 
        ,borrowing_dispersion 
        ,platform_state
        FROM
        dw.qyxg_platform_data
        WHERE
        dt = '{version}'
        '''.format(
            version=PLATFORM_VERSION
        )
    )
    tid_platform_df = platform_df.select(
        'bbd_qyxx_id',
        'company_name',
        'platform_name',
        'platform_state',
        get_float_udf('per_lending_amount').alias('p2p_feature_1'),
        get_float_udf('avg_soldout_time').alias('p2p_feature_2'),
        get_float_udf('total_num_of_lender').alias('p2p_feature_3'),
        get_float_udf('total_turnover').alias('p2p_feature_4'),
        get_float_udf('total_deal_volume').alias('p2p_feature_5'),
        json_to_obj_udf(
            'monthly_deal_data').getItem(
                'deal_volume').alias('p2p_feature_6'),
        json_to_obj_udf(
            'monthly_deal_data').getItem(
                'turnover').alias('p2p_feature_7'),
        json_to_obj_udf(
            'monthly_deal_data').getItem(
                'nominal_interest_rate').alias('p2p_feature_8'),
        json_to_obj_udf(
            'monthly_deal_data').getItem(
                'num_of_lender').alias('p2p_feature_9'),
        json_to_obj_udf(
            'monthly_deal_data').getItem(
                'num_of_borrower').alias('p2p_feature_10'),
        get_float_udf('per_lending_num').alias('p2p_feature_11'),
        get_float_udf('avg_lend_time').alias('p2p_feature_12'),
        get_float_udf('per_borrowing_num').alias('p2p_feature_13'),
        get_float_udf('total_num_of_borrower').alias('p2p_feature_14'),
        get_float_udf('lending_dispersion').alias('p2p_feature_15'),
        get_float_udf('loan_balance').alias('p2p_feature_16'),
        get_float_udf('per_borrowing_amount').alias('p2p_feature_17'),
        get_float_udf('borrowing_dispersion').alias('p2p_feature_18'),
    )
    
    prd_platform_df = tid_platform_df.join(
        tid_wdzj_df,
        [tid_platform_df.platform_name == tid_wdzj_df.platform_name,
         tid_platform_df.company_name == tid_wdzj_df.company_name],
        'outer'
    ).select(
        get_unique_string_udf(
            tid_platform_df.bbd_qyxx_id,
            tid_wdzj_df.bbd_qyxx_id
        ).alias('bbd_qyxx_id'),
        get_unique_string_udf(
            tid_platform_df.company_name,
            tid_wdzj_df.company_name
        ).alias('company_name'),
        get_unique_string_udf(
            tid_platform_df.platform_name,
            tid_wdzj_df.platform_name
        ).alias('platform_name'),
        get_unique_string_udf(
            tid_platform_df.platform_state,
            tid_wdzj_df.platform_state
        ).alias('platform_state'),
        'p2p_feature_1',
        'p2p_feature_2',
        'p2p_feature_3',
        'p2p_feature_4',
        'p2p_feature_5',
        'p2p_feature_6',
        'p2p_feature_7',
        'p2p_feature_8',
        'p2p_feature_9',
        'p2p_feature_10',
        'p2p_feature_11',
        'p2p_feature_12',
        'p2p_feature_13',
        'p2p_feature_14',
        'p2p_feature_15',
        'p2p_feature_16',
        'p2p_feature_17',
        'p2p_feature_18',
        tid_wdzj_df.p2p_feature_19,
        tid_wdzj_df.p2p_feature_20,
        tid_wdzj_df.p2p_feature_21
    ).dropDuplicates(
        ['bbd_qyxx_id', 'platform_name']
    ).fillna(
        0
    )
    
    return prd_platform_df

def run(relation_version):
    '''
    格式化输出
    '''
    pd_df = spark_data_flow()
    os.system(
        ("hadoop fs -rmr "
         "{path}/"
         "p2p_feature_distribution/{version}").format(path=OUT_PATH, 
                                                      version=relation_version))
    pd_df.repartition(10).write.json(
        ("{path}/"
         "p2p_feature_distribution/{version}").format(path=OUT_PATH, 
                                                      version=relation_version))
def get_spark_session():
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 15)
    conf.set("spark.executor.memory", "20g")
    conf.set("spark.executor.instances", 30)
    conf.set("spark.executor.cores", 5)
    conf.set("spark.python.worker.memory", "3g")
    conf.set("spark.default.parallelism", 600)
    conf.set("spark.sql.shuffle.partitions", 600)
    conf.set("spark.broadcast.blockSize", 1024)
    conf.set("spark.executor.extraJavaOptions",
             "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")    
    conf.set("spark.submit.pyFiles", 
             "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/dafei_keyword.py")
    conf.set("spark.files", 
                ("hdfs://bbdc6ha/user/antifraud/source/keyword_demo/city,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/1gram.words,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/2gram.words,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/new.work.words"))
    
    spark = SparkSession \
        .builder \
        .appName("hongjing2_one_prd_p2p") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()    
    
    return spark
    
if __name__ == '__main__':  
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #输入参数
    PLATFORM_VERSION = conf.get('p2p_company_feature', 'PLATFORM_VERSION')
    WDZJ_VERSION = conf.get('p2p_company_feature', 'WDZJ_VERSION')
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    OUT_PATH = conf.get('common_company_feature', 'OUT_PATH')
    
    spark = get_spark_session()
    
    run(relation_version=RELATION_VERSION)
    