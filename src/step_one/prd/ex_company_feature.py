# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
ex_company_feature.py {version}

'''

import sys
import os

import configparser
from pyspark.sql import types as tp
from pyspark.sql import functions as fun
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


    
class ExFeatureConstruction(object):
    '''
    计算特征的函数集
    '''      
    
    def __fault_tolerant(func):
        '''
        一个用于容错的装饰器
        '''
        @classmethod
        def wappen(cls, *args, **kwargs):
            try:
                return func(cls, *args, **kwargs)
            except Exception:
                return {'error': 'error'}
        return wappen    
    
    @__fault_tolerant
    def get_feature_1(cls, gov_doc):
        '''
        政府批文
        '''
        if (gov_doc == 'NULL' or 
                gov_doc == 'null' or not gov_doc):
            risk = 0.
        else:
            risk = 1.
            
        return float(risk)   
    
    @__fault_tolerant
    def get_feature_2(cls, regcap):
        '''
        注册资本（万元）
        '''
        try:
            return float(regcap)
        except:
            return 0.

    @__fault_tolerant
    def get_feature_3(cls, exchange_type):
        '''
        交易所类型
        '''
        if exchange_type == u'现货':
            risk = 1.
        elif exchange_type == u'权益':
            risk = 2.
        else:
            risk = 3.
        
        return float(risk)
    
    @__fault_tolerant
    def get_feature_4(cls, material_files):
        '''
        资料文件   
        '''
        if (material_files == 'NULL' or 
                material_files == 'null' or not material_files):
            risk = 0.
        else:
            risk = 1.
            
        return float(risk)  
    
    @__fault_tolerant
    def get_feature_5(cls, legal_opinion):
        '''
        违规会员单位风险
        '''
        pass

    @__fault_tolerant
    def get_feature_6(cls, legal_opinion):
        '''
        高风险会员单位风险
        '''
        pass    
    
        
class SparkUdf(ExFeatureConstruction):
    '''
    定义Spark-udf
    '''
    @classmethod
    def define_spark_udf(cls, func_num, return_type):
        func_name = eval(
            'cls.get_feature_{func_num}'.format(func_num=func_num))
        return fun.udf(func_name, return_type)    


def spark_data_flow(exchange_version):
    exchange_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        gov_doc,
        exchange_type,
        regcap,
        material_files
        FROM
        dw.qyxg_exchange
        WHERE
        dt='{version}'
        '''.format(version=exchange_version)
    ).cache()
    
    udf_return_type = tp.FloatType()
    tid_df = exchange_df.select(
        'bbd_qyxx_id',
        'company_name',
        SparkUdf.define_spark_udf(
            1, udf_return_type)('gov_doc').alias('ex_feature_1'),
        SparkUdf.define_spark_udf(
            2, udf_return_type)('regcap').alias('ex_feature_2'),
        SparkUdf.define_spark_udf(
            3, udf_return_type)('exchange_type').alias('ex_feature_3'),
        SparkUdf.define_spark_udf(
            4, udf_return_type)('material_files').alias('ex_feature_4')
    )
    
    return tid_df
    
def run(exchange_version, relation_version):
    '''
    格式化输出
    '''
    pd_df = spark_data_flow(exchange_version)
    os.system(
        ("hadoop fs -rmr "
         "{path}/"
         "ex_feature_distribution/{version}").format(path=OUT_PATH, 
                                                     version=relation_version))
    pd_df.repartition(10).write.json(
        ("{path}/"
         "ex_feature_distribution/{version}").format(path=OUT_PATH, 
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
        .appName("hongjing2_one_prd_ex") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()    
    
    return spark
    
if __name__ == '__main__':  
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #输入参数
    EXCHANGE_VERSION = conf.get('ex_company_feature', 'EXCHANGE_VERSION')
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    OUT_PATH = conf.get('common_company_feature', 'OUT_PATH')
    
    spark = get_spark_session()
    
    run(
        exchange_version=EXCHANGE_VERSION,
        relation_version=RELATION_VERSION
    )
    