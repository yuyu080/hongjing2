# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
ra_company.py
'''

import json

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp


def get_risk_sequence(*args):
    result = {}
    for each_args in args:
        each_version, each_score = each_args.split(':')
        result[each_version] = each_score
    return json.dumps(result, ensure_ascii=False)

def get_xgxx_change(old_xgxx, new_xgxx):
    '''
    获取相关信息的变动情况，并按照一定格式输出
    '''
    if old_xgxx and new_xgxx:
        old_xgxx_obj = json.loads(old_xgxx)
        new_xgxx_obj = json.loads(new_xgxx)
        
        all_keys = new_xgxx_obj.keys()
        
        #获取最终输出列表
        result = [
            {
                "type": each_key,
                "value": new_xgxx_obj[each_key],
                "isupdate": get_risk_change(
                    old_xgxx_obj[each_key],
                    new_xgxx_obj[each_key]
                )
            }
            for each_key in all_keys
        ]
        return  json.dumps(result, ensure_ascii=False)
    else:
        return u'无'

def get_risk_change(old_score, new_score):
    if old_score and new_score:
        is_rise = new_score - old_score
    else:
        is_rise = 0.
    if is_rise < 0:
        return -1
    elif is_rise > 0:
        return 1
    else:
        return 0

def tid_spark_data_flow():
    '''
    构建df的某些字段
    '''
    #构建udf
    get_risk_change_udf = fun.udf(get_risk_change, tp.IntegerType())
    get_xgxx_change_udf = fun.udf(get_xgxx_change, tp.StringType())
    get_risk_sequence_udf = fun.udf(get_risk_sequence, tp.StringType())
    
    
    #原始输入
    old_df =  spark.read.parquet(
        ("/user/antifraud/hongjing2/dataflow/step_three/prd"
        "/all_company_info/{version}").format(version=OLD_VERSION))
    new_df =  spark.read.parquet(
        ("/user/antifraud/hongjing2/dataflow/step_three/prd"
        "/all_company_info/{version}").format(version=NEW_VERSION))
        
    #易燃指数是否上升
    tmp_new_df = new_df.select(
        'company_name',
        'risk_index'
    )
    tmp_old_df = old_df.select(
        'company_name',
        'risk_index'
    )
    tmp_new_2_df = tmp_new_df.join(
        tmp_old_df,
        'company_name',
        'left_outer'
    ).select(
        'company_name',
        tmp_new_df.risk_index.alias('new'),
        tmp_old_df.risk_index.alias('old'),
        get_risk_change_udf(
            tmp_old_df.risk_index, 
            tmp_new_df.risk_index
        ).alias('is_rise')
    )

    #相关信息变更情况
    tmp_new_3_df = new_df.select(
        'company_name',
        'xgxx_info'
    )
    tmp_old_3_df = old_df.select(
        'company_name',
        'xgxx_info'
    )
    tmp_new_4_df = tmp_new_3_df.join(
        tmp_old_3_df,
        'company_name',
        'left_outer'
    ).select(
        'company_name',
        tmp_new_3_df.xgxx_info,
        tmp_old_3_df.xgxx_info,
        get_xgxx_change_udf(
            tmp_old_3_df.xgxx_info,
            tmp_new_3_df.xgxx_info
        ).alias('xgxx_info_with_change')
    )
    
    #易燃指数时序图
    tmp_new_5_df = new_df.select(
        'company_name',
        fun.concat_ws(
            ':', 'data_version', 'risk_index'
        ).alias('risk_with_version'),
    )
    tmp_old_5_df = old_df.select(
        'company_name',
        fun.concat_ws(
            ':', 'data_version', 'risk_index'
        ).alias('risk_with_version'),
    )
    tmp_new_6_df = tmp_new_5_df.join(
        tmp_old_5_df,
        'company_name',
        'left_outer'
    ).select(
        tmp_new_5_df.company_name,
        get_risk_sequence_udf(
            tmp_new_5_df.risk_with_version,
            tmp_old_5_df.risk_with_version
        ).alias('risk_sequence_version')
    )
    
    #组合所有字段最终输出
    tid_new_df = new_df.join(
        tmp_new_2_df,
        'company_name',
        'left_outer'
    ).join(
        tmp_new_4_df,
        'company_name',
        'left_outer'
    ).join(
        tmp_new_6_df,
        'company_name',
        'left_outer'
    ).select(
        new_df.bbd_qyxx_id,
        new_df.province,
        new_df.city,
        new_df.county,
        new_df.company_name,
        new_df.risk_index,
        new_df.risk_rank,
        tmp_new_2_df.is_rise,
        new_df.company_type,
        new_df.risk_composition,
        new_df.risk_tags,
        tmp_new_6_df.risk_sequence_version,
        tmp_new_4_df.xgxx_info_with_change
    )

    return tid_new_df
    
def spark_data_flow():
    '''
    构建最终输出df
    '''
    new_df =  spark.read.parquet(
        ("/user/antifraud/hongjing2/dataflow/step_three/prd"
        "/all_company_info/{version}").format(version=NEW_VERSION))
        
    tid_new_df = tid_spark_data_flow()

    
    prd_new_df = tid_new_df.where(
        new_df.bbd_qyxx_id.isNotNull()
    ).select(
        new_df.bbd_qyxx_id.alias('id'),
        'province',
        'city',
        tid_new_df.county.alias('area'),
        tid_new_df.company_name.alias('company'),
        'risk_index',
        tid_new_df.risk_rank.alias('risk_level'),
        tid_new_df.is_rise.alias('rise'),
        tid_new_df.company_type.alias('industry'),
        tid_new_df.risk_composition.alias('index_radar'),
        tid_new_df.risk_tags.alias('risk_scan'),
        tid_new_df.risk_sequence_version.alias('index_sort'),
        tid_new_df.xgxx_info_with_change.alias('company_detail'),
        fun.current_timestamp().alias('gmt_create'),
        fun.current_timestamp().alias('gmt_update')
    ).fillna(
        u'无'
    ).fillna(
        {'city': u'无', 'area': u'无', 'province': u'无'}
    ).dropDuplicates(
        ['id']
    )    
    return prd_new_df

def run():
    prd_df = spark_data_flow()
    prd_df.repartition(
        20
    ).write.jdbc(url=URL, table=TABLE, 
                 mode="append", properties=PROP)
    print '\n************\n导入大成功SUCCESS !!\n************\n'

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
        .appName("hgongjing2_four_raw_all_info") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 

if __name__ == '__main__':
    #用于比较的两个数据版本
    OLD_VERSION = '20170117'
    NEW_VERSION = '20170403'
    
    #mysql输出信息
    TABLE = 'ra_company'
    URL = "jdbc:mysql://10.10.20.180:3306/airflow?characterEncoding=UTF-8"
    PROP = {"user": "airflow", 
            "password":"airflow", 
            "driver": "com.mysql.jdbc.Driver"}
    
    spark = get_spark_session()
    
    run()