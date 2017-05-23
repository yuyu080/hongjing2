# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
ra_gather_place.py
'''
import sys
import os
import json

import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql import functions as fun
from pyspark.sql import types as tp


def get_relation(col):
    '''
    将数据格式化
    '''
    result = {
        '1': [],
        '2': [],
        '3': []
    }
    for each_info in col:
        degree, province = each_info.split(':')
        if int(degree):
            result[degree].append(province) 
    return json.dumps(result, ensure_ascii=False)

def spark_data_flow():
    #原始关联方
    relation_df = spark.sql(
        '''SELECT 
        bbd_qyxx_id,
        source_bbd_id,
        destination_bbd_id,
        company_name,
        source_degree,
        destination_degree
        FROM 
        dw.off_line_relations 
        WHERE 
        dt='{version}'  
        AND
        source_degree <= 3
        AND
        destination_degree <= 3
        '''.format(version=RELATION_VERSION)
    ).dropDuplicates(
        ['bbd_qyxx_id', 'source_bbd_id', 'destination_bbd_id']
    ).cache()
    
    #找出独立的节点
    tmp_relation_df = relation_df.select(
        'bbd_qyxx_id',
        'company_name',
        relation_df.source_bbd_id.alias('id'),
        relation_df.source_degree.alias('degree')
    ).union(
        relation_df.select(
            'bbd_qyxx_id',
            'company_name',
            relation_df.destination_bbd_id.alias('id'),
            relation_df.destination_degree.alias('degree')
        )
    ).dropDuplicates(
    )
    
    #企业省份信息
    basic_df = spark.read.parquet(
        ('/user/antifraud/hongjing2/dataflow/step_one'
         '/raw/basic/{version}').format(version=RELATION_VERSION)
    )
    province_df = basic_df.select(
        'bbd_qyxx_id',
        'company_province'
    )

    #找出某企业各度关联方的前3省份
    get_relation_udf = fun.udf(get_relation, tp.StringType()) 

    tid_relation_df = province_df.join(
        tmp_relation_df,
        province_df.bbd_qyxx_id == tmp_relation_df.id
    ).select(
        tmp_relation_df.bbd_qyxx_id,
        tmp_relation_df.company_name,
        tmp_relation_df.id,
        tmp_relation_df.degree,
        province_df.company_province
    ).groupBy(
        ['bbd_qyxx_id', 'company_name', 
         'degree', 'company_province']
    ).count(
    ).withColumnRenamed(
        'count', 'num'
    ).cache()
    
    window = Window.partitionBy(
        ['bbd_qyxx_id', 'degree']
    ).orderBy(
        tid_relation_df.num.desc()
    )
    
    tid_relation_2_df = tid_relation_df.select(
        'bbd_qyxx_id', 
        'company_name', 
        'degree', 
        'company_province',
        'num',
        fun.concat_ws(
            ':', 'degree', 'company_province'
        ).alias('degree_with_province'),
        row_number().over(window).alias('row_number')
    ).where(
        'row_number <= 3'
    ).groupBy(
        ['bbd_qyxx_id', 'company_name']
    ).agg(
        {'degree_with_province': 'collect_set'}
    ).select(
        'bbd_qyxx_id',
        'company_name',
        get_relation_udf(
            'collect_set(degree_with_province)'
        ).alias('degree_province_distribution')
    )
    
    prd_df = tid_relation_2_df.select(
        tid_relation_2_df.bbd_qyxx_id.alias('id'),
        tid_relation_2_df.degree_province_distribution.alias('gather_place'),
        fun.current_timestamp().alias('gmt_create'),
        fun.current_timestamp().alias('gmt_update')        
    ).dropDuplicates(
        ['id']
    )
        
    return prd_df
    
def run():
    prd_df = spark_data_flow()
    
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "ra_gather_place").format(path=OUT_PATH))
    prd_df.repartition(
        20    
    ).rdd.map(
        lambda r:
        '\t'.join([
            r.id,
            r.gather_place,
            r.gmt_create.strftime('%Y-%m-%d %H:%M:%S'),
            r.gmt_update.strftime('%Y-%m-%d %H:%M:%S')
        ])
    ).saveAsTextFile(
        "{path}/ra_gather_place".format(path=OUT_PATH)
    )
        
    #输出到mysql
    os.system(
    ''' 
    sqoop export \
    --connect {url} \
    --username {user} \
    --password '{password}' \
    --table {table} \
    --export-dir {path}/{table} \
    --input-fields-terminated-by '\\t' 
    '''.format(
        url=URL,
        user=PROP['user'],
        password=PROP['password'],
        table=TABLE,
        path=OUT_PATH
    )
    )
    
    print '\n************\n导入大成功SUCCESS !!\n************\n'

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "15g")
    conf.set("spark.executor.instances", 50)
    conf.set("spark.executor.cores", 4)
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
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")

    #数据版本
    RELATION_VERSION = sys.argv[1]
    #输出
    OUT_PATH = '/user/antifraud/hongjing2/dataflow/step_four/raw'    
    
    #mysql输出信息
    TABLE = 'ra_gather_place'
    URL = conf.get('mysql', 'URL')
    PROP = eval(conf.get('mysql', 'PROP'))
    
    spark = get_spark_session()
    
    run()