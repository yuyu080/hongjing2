# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
all_data_preparation.py {version}
'''

import sys
import os

from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.conf import SparkConf
from pyspark.sql import Row, SparkSession


def get_company_namefrag(iterator):
    '''
    构建DAG；这里因为涉及到加载词典，只能用mapPartition，不然IO开销太大
    '''
    try:
        from dafei_keyword import KeywordExtr
        _obj = KeywordExtr("city", "1gram.words", 
                           "2gram.words", "new.work.words")
        keyword_list = []
        for row in iterator:
            keyword_list.append(
                (row.company_name, _obj.clean(row.company_name)))
        return keyword_list
    except Exception, e:
        return e

def get_relation_type(relations):
    if 'INVEST' in relations:
        return 'INVEST'
    else:
        return 'UNINVEST'

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "25g")
    conf.set("spark.executor.instances", 30)
    conf.set("spark.executor.cores", 5)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 1000)
    conf.set("spark.sql.shuffle.partitions", 1000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)
    conf.set("spark.submit.pyFiles", 
             "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/dafei_keyword.py")
    conf.set("spark.files", 
                ("hdfs://bbdc6ha/user/antifraud/source/keyword_demo/city,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/1gram.words,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/2gram.words,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/new.work.words"))
    
    spark = SparkSession \
        .builder \
        .appName("hgongjing2_one_prd_common_static_v2") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark
    
def spark_data_flow(tidversion):
    '''
    dataflow
    '''
    get_relation_type_udf = fun.udf(get_relation_type, tp.StringType())
    
    #基础数据
    basic_df = spark.read.parquet(
        "{path}/basic/{version}".format(version=RELATION_VERSION,
                                        path=IN_PATH)
    )
    
    #公司字号
    namefrag_df = basic_df.select(
        'company_name'
    ).rdd.repartition(
        100
    ).mapPartitions(
        get_company_namefrag
    ).map(
        lambda r: Row(company_name=r[0], namefrag=r[1])
    ).toDF(
    )

    #关联方数据
    raw_relation_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id                  a,
        source_bbd_id               b,
        destination_bbd_id        c,
        company_name              a_name,
        source_name                  b_name,
        destination_name           c_name,
        source_degree               b_degree,
        destination_degree       c_degree,
        relation_type                 bc_relation,
        source_isperson            b_isperson,
        destination_isperson    c_isperson
        FROM 
        dw.off_line_relations
        WHERE 
        dt='{version}'  
        AND
        source_degree <= 3
        AND
        destination_degree <= 3
        '''.format(version='20170518')
    )
    
    tid_relation_df = raw_relation_df.groupBy(
        ['a', 'b', 'c', 'a_name', 'b_name', 'c_name', 
         'b_degree', 'c_degree', 'b_isperson', 'c_isperson']
    ).agg(
        {'bc_relation': 'collect_list'}
    ).select(
        'a', 'b', 'c',
        'a_name', 'b_name', 'c_name', 
        'b_degree', 'c_degree', 
        'b_isperson', 'c_isperson',
        get_relation_type_udf(
            'collect_list(bc_relation)'
        ).alias('bc_relation')
    )
        
    return namefrag_df, tid_relation_df

def run():
    '''
    格式化输出
    '''
    namefrag_df, tid_relation_df = spark_data_flow(RELATION_VERSION)

    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "all_namefrag"
         "/{version}").format(path=OUT_PATH, 
                              version=RELATION_VERSION))    
    namefrag_df.repartition(
        10
    ).write.parquet(
        "{path}/"
        "all_namefrag/{version}".format(path=OUT_PATH, 
                                        version=RELATION_VERSION)
    )

    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "all_relation"
         "/{version}").format(path=OUT_PATH, 
                              version=RELATION_VERSION))    
    tid_relation_df.coalesce(
        30
    ).write.parquet(
        "{path}/"
        "all_relation/"
        "{version}".format(path=OUT_PATH, 
                           version=RELATION_VERSION)
    )
    

if __name__ == '__main__':  
    import configparser
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    #输入参数
    IN_PATH = "/user/antifraud/hongjing2/dataflow/step_one/raw/"
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_four/tid/raw/"
    
    spark = get_spark_session()

    run()