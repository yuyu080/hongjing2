# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
ra_black_white.py {version}
'''
import os
import sys

import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Window
from pyspark.sql.functions import rank
from pyspark.sql import functions as fun
from pyspark.sql import types as tp

def is_black(platform_state):
    '''判断是否是黑企业'''
    if (platform_state == u'正常' or 
        platform_state == 'NULL'):
        return 0
    else:
        return 1

def spark_data_flow():
    is_black_udf = fun.udf(is_black, tp.IntegerType())

    raw_wdzj_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        platform_name,
        platform_state,
        2 priority
        FROM
        dw.qyxg_wdzj
        WHERE
        dt='{version}'
        '''.format(version=WDZJ_VERSION)
    )
    platform_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id
        ,company_name
        ,platform_name
        ,platform_state
        ,1 priority
        FROM
        dw.qyxg_platform_data
        WHERE
        dt = '{version}'
        '''.format(
            version=PLATFORM_VERSION
        )
    )
    
    tid_df = raw_wdzj_df.union(
        platform_df
    ).withColumn(
        'is_black', is_black_udf('platform_state')
    )
    
    window = Window.partitionBy(
        "bbd_qyxx_id"
    ).orderBy(
        tid_df.priority.desc()
    )
    
    prd_df = tid_df.select(
        'bbd_qyxx_id',
        'company_name',
        'platform_name',
        'platform_state',
        'is_black',
        'priority',
        rank().over(window).alias('rank')
    ).where(
        'rank == 1'
    ).select(
        tid_df.bbd_qyxx_id.alias('id'),
        tid_df.company_name.alias('company'),
        tid_df.platform_state.alias('reason'),
        tid_df.is_black.alias('is_black'), 
        fun.current_timestamp().alias('gmt_create'),
        fun.current_timestamp().alias('gmt_update')
    ).dropDuplicates(
        ['id']
    )

    return prd_df
 
def run():
    '''
    格式化输出
    '''
    prd_df = spark_data_flow()

    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "ra_black_white").format(path=OUT_PATH))
    prd_df.where(
        prd_df.id.isNotNull()
    ).repartition(
        10    
    ).rdd.map(
        lambda r:
        '\t'.join([
            r.id,
            r.company,
            r.reason,
            str(r.is_black),
            r.gmt_create.strftime('%Y-%m-%d %H:%M:%S'),
            r.gmt_update.strftime('%Y-%m-%d %H:%M:%S')
        ])
    ).saveAsTextFile(
        "{path}/ra_black_white".format(path=OUT_PATH)
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

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "20g")
    conf.set("spark.executor.instances", 15)
    conf.set("spark.executor.cores", 5)
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
    
    #输入的P2P名单
    PLATFORM_VERSION = conf.get('p2p_company_feature', 'PLATFORM_VERSION')
    WDZJ_VERSION = conf.get('p2p_company_feature', 'WDZJ_VERSION')
    RELATION_VERSION = sys.argv[1]
    OUT_PATH = '/user/antifraud/hongjing2/dataflow/step_four/raw'
    
    #mysql输出信息
    TABLE = 'ra_black_white'
    URL = conf.get('mysql', 'URL')
    PROP = eval(conf.get('mysql', 'PROP'))
    
    spark = get_spark_session()
    
    run()