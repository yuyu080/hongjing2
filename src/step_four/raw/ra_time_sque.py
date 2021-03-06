# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
--queue project.hongjing \
ra_time_sque.py
'''
import json
import numpy as np
import os

import MySQLdb
import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.sql import Row

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

def get_median(iter_obj):
    '''
    取中位数
    '''
    score_list = list(iter_obj)
    return round(
        np.median(score_list),
        1
    )

def get_avg(iter_obj):
    '''
    取平均数
    '''
    score_list = list(iter_obj)
    return round(
        np.average(score_list),
        1
    )

def recursion_data(all_version_data):
    '''多个rdd的聚合数据，尼玛的要递归求值'''
    all_data = []
    def get_all_obj(data):
        all_data.append(data[1])
        if len(data[0]) != 2:
            all_data.append(data[0])
        elif len(data[0]) == 2:
            get_all_obj(data[0])
    get_all_obj(all_version_data)
    return all_data

def get_sequence_info(industry_type, iter_objs):
    '''
    得到rdd的某一行 
    '''
    result = []
    avg_sequence_dict = dict()
    median_sequence_dict = dict()
    for each_obj in iter_objs:
        value_dict = each_obj
        avg_sequence_dict[
            value_dict['data_version']
        ] = value_dict['avg']
        median_sequence_dict[
            value_dict['data_version']
        ] = value_dict['median']                                    
    result.append(
        [u'行业中位数', 
         industry_type, 
         json.dumps(median_sequence_dict)]
    )
    result.append(
        [u'行业平均数', 
         industry_type, 
         json.dumps(avg_sequence_dict, ensure_ascii=False)]
    )
    return result

def get_industry_score(df, version):
    '''
    取某个版本的行业中位数、平均值
    '''
    out_df = df.rdd.map(
        lambda r: (r.company_type, r.risk_index)
    ).groupByKey(
    ).map(
        lambda (k, v): (k, 
                        dict(avg=get_avg(v),
                        median=get_median(v),
                        data_version=version))
    )
    return out_df
    
def get_df(version):
    '''
    获取单个版本的df
    '''
    df = spark.read.parquet(
        ("{path}"
         "/all_company_info/{version}").format(path=IN_PATH,
                                               version=version))
    #排除网络借贷中的黑企业
    df = df.where(
        df.risk_index != 100
    )
    return df

def raw_spark_data_flow(version_list):
    '''
    输入每个一个版本列表，计算风险时序，输出一个最终rdd
    '''
    rdd_list = []
    for each_version in version_list:
        each_df = get_df(each_version)
        each_rdd = get_industry_score(each_df, each_version)
        rdd_list.append(each_rdd)

    tid_rdd = eval(
        "rdd_list[{0}]".format(0) + 
        "".join([
                ".join(rdd_list[{0}])".format(rdd_index) 
                for rdd_index in range(1, len(rdd_list))])
    ).mapValues(
        recursion_data
    )
        
    prd_rdd = tid_rdd.flatMap(
        lambda (k ,v): get_sequence_info(k, v)
    )
    return prd_rdd

def get_id():
    return ''

def spark_data_flow():
    get_id_udf = fun.udf(get_id, tp.StringType())
    
    prd_rdd = raw_spark_data_flow(VERSION_LIST)
    raw_df = prd_rdd.map(
        lambda r: Row(
            val_type=r[0],
            industry=r[1],
            time_sque=r[2]
        )
    ).toDF()

    prd_df = raw_df.select(
        get_id_udf().alias('id'),
        'val_type',
        'industry',
        'time_sque',
        fun.current_timestamp().alias('gmt_create'),
        fun.current_timestamp().alias('gmt_update')
    )    
    return prd_df

def run():
    prd_df = spark_data_flow()
    
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "ra_time_sque").format(path=OUT_PATH))
    
    prd_df.repartition(
        10
    ).rdd.map(
        lambda r:
        '\t'.join([
            r.id,
            r.val_type,
            r.industry,
            r.time_sque,
            r.gmt_create.strftime('%Y-%m-%d %H:%M:%S'),
            r.gmt_update.strftime('%Y-%m-%d %H:%M:%S')
        ])
    ).saveAsTextFile(
        "{path}/ra_time_sque".format(path=OUT_PATH)
    )
    
    #输出到mysql
    if IS_INTO_MYSQL:
        truncate_table('ra_time_sque')
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
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #所有数据版本,取最近6个月
    VERSION_LIST = eval(conf.get('common', 'RELATION_VERSIONS'))[-6:]

    #结果存一份在HDFS，同时判断是否输出到mysql
    IN_PATH = conf.get('all_company_info', 'OUT_PATH')
    OUT_PATH = conf.get('to_mysql', 'OUT_PATH')
    IS_INTO_MYSQL = conf.getboolean('to_mysql', 'IS_INTO_MYSQL')
    
    #mysql输出信息
    TABLE = 'ra_time_sque'
    URL = conf.get('mysql', 'URL')
    PROP = eval(conf.get('mysql', 'PROP'))
    
    spark = get_spark_session()
    
    run()