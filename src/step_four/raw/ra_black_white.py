# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
--queue project.hongjing \
ra_black_white.py {version}
'''
import os
import sys

import MySQLdb
import configparser

from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp


def truncate_table(table):
    '''连接mysql，执行一个SQL'''
    db = MySQLdb.connect(host=PROP['ip'], user=PROP['user'], 
                         passwd=PROP['password'], db=PROP['db_name'], 
                         charset="utf8")
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    # 使用execute方法执行SQL语句
    sql = "TRUNCATE TABLE {0}".format(table)
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
    # 关闭数据库连接
    db.close()
    
    print "清空表{0}成功".format(table)

def is_black(platform_state):
    '''判断是否是黑企业'''
    if (platform_state == u'经侦介入' or 
        platform_state == u'提现困难' or
        platform_state == u'跑路'):
        return 1
    else:
        return 0

def get_join_souce(col):
    return u'网络爬取'

def get_join_date(col):
    return ''

def spark_data_flow():
    is_black_udf = fun.udf(is_black, tp.IntegerType())
    get_join_souce_udf = fun.udf(get_join_souce, tp.StringType())
    get_join_date_udf = fun.udf(get_join_date, tp.StringType())
    
    raw_wdzj_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        platform_name,
        platform_state
        FROM
        dw.qyxg_wdzj
        WHERE
        dt='{version}'
        '''.format(version=WDZJ_VERSION)
    )
    raw_basic_df = spark.read.parquet(
        ("{path}"
         "/basic/{version}").format(path=IN_PATH,
                                    version=RELATION_VERSION))
    county_mapping_df = spark.read.csv(
        "{path}".format(path=MAPPING_PATH), 
        sep='\t', 
        header=True)
    
    tid_df = raw_wdzj_df.join(
        raw_basic_df,
        raw_basic_df.bbd_qyxx_id == raw_wdzj_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        county_mapping_df,
        county_mapping_df.code == raw_basic_df.company_county,
        'left_outer'
    ).select(
        raw_wdzj_df.bbd_qyxx_id,
        fun.when(
            raw_basic_df.company_name.isNotNull(), 
            raw_basic_df.company_name
        ).otherwise(
            raw_wdzj_df.company_name
        ).alias('company_name'),
        raw_wdzj_df.platform_name,
        raw_wdzj_df.platform_state,
        county_mapping_df.province,
        county_mapping_df.city,
        county_mapping_df.county
    )
    
    tid_df = tid_df.withColumn(
        'is_black', is_black_udf('platform_state')
    ).withColumn(
        'join_souce', get_join_souce_udf('platform_state')
    ).withColumn(
        'join_date', get_join_date_udf('platform_state')
    )
    
    
    window = Window.partitionBy(
        "bbd_qyxx_id"
    ).orderBy(
        tid_df.is_black.desc()
    )
    
    prd_df = tid_df.select(
        tid_df.bbd_qyxx_id.alias('id'),
        tid_df.company_name.alias('company'),
        tid_df.platform_state.alias('reason'),
        tid_df.is_black, 
        tid_df.join_souce,
        tid_df.join_date,
        tid_df.province,
        tid_df.city,
        tid_df.county.alias('area'),
        fun.current_timestamp().alias('gmt_create'),
        fun.current_timestamp().alias('gmt_update'),
        row_number().over(window).alias('rank')
    ).where(
        'rank == 1'    
    ).fillna(
        {'city': u'无', 'area': u'无', 'province': u'无'}
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
            r.join_souce,
            r.join_date,
            r.province,
            r.city,
            r.area,
            r.gmt_create.strftime('%Y-%m-%d %H:%M:%S'),
            r.gmt_update.strftime('%Y-%m-%d %H:%M:%S')
        ])
    ).saveAsTextFile(
        "{path}/ra_black_white".format(path=OUT_PATH)
    )

    #输出到mysql
    if IS_INTO_MYSQL:
        truncate_table('ra_black_white')
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
    
    #结果存一份在HDFS，同时判断是否输出到mysql
    IN_PATH = conf.get('common_company_info', 'OUT_PATH')
    IS_INTO_MYSQL = conf.getboolean('to_mysql', 'IS_INTO_MYSQL')
    MAPPING_PATH = conf.get('info_merge', 'MAPPING_PATH')
    OUT_PATH = conf.get('to_mysql', 'OUT_PATH')
    
    #mysql输出信息
    TABLE = 'ra_black_white'
    URL = conf.get('mysql', 'URL')
    PROP = eval(conf.get('mysql', 'PROP'))
    
    spark = get_spark_session()
    
    run()