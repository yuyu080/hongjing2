# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
ex_company_feature.py

'''


import json
import re
import os

from pyspark.sql import types as tp
from pyspark.sql import functions as fun
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_float(value):
    try:
        return round(float(re.search('[\d\.\,]+', 
                                     value).group().replace(',', '')), 2)
    except:
        return 0.
    
    
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
            except Exception, e:
                return dict(error=(
                    "{func_name} has a errr : {excp}"
                ).format(func_name=func.__name__, excp=e))
        return wappen    
    
    @__fault_tolerant
    def get_json_key(cls, json_string, key):
        '''
        从json字符串中求出某个key的平均值
        '''
        obj = json.loads(json_string)
        data = [get_float(each_line.get(key, '0'))
                     for each_line in obj]
        total = float(sum(data))
        num = len(data) if total else 0
        avg = round(total/num, 2) if num else 0.
        return avg
    
    @__fault_tolerant
    def get_feature_1(cls, trading_variety_info):
        '''
        交易波动风险
        '''
        avg = cls.get_json_key(trading_variety_info, u'每日波动价格限制')
        
        if 0 < avg < 5:
            risk = 10.
        elif 5 <= avg < 10:
            risk = 30.
        elif 10 <= avg < 50:
            risk = 60.
        elif 50 <= avg:
            risk = 100.
        else:
            risk = 10.
        
        return risk

    @__fault_tolerant
    def get_feature_2(cls, trading_variety_info):
        '''
        保证金风险
        '''
        avg = cls.get_json_key(trading_variety_info, u'最低保证金')
        
        if 0 < avg < 5:
            risk = 100.
        elif 5 <= avg < 8:
            risk = 60.
        elif 8 <= avg < 15:
            risk = 30.
        elif 15 <= avg:
            risk = 10.
        else:
            risk = 100.
        
        return risk
    
    @__fault_tolerant
    def get_feature_3(cls, trading_variety_info):
        '''
        手续费风险
        '''
        def get_sp_float(data):
            try:
                return round(float(re.search('[\d\.\,]+%', 
                                             data).group().replace('%', '')), 2)
            except:
                return 0.            
        
        obj = json.loads(trading_variety_info)
        data = [get_sp_float(each_line.get(u'手续费', '0'))
                     for each_line in obj]
        total = float(sum(data))
        num = len(filter(lambda x: x != 0, data)) if total else 0
        avg = round(total/num, 2) if num else 0.
        
        if 0 < avg < 0.05:
            risk = 100.
        elif 0.05 <= avg < 0.1:
            risk = 60.
        elif 0.1 <= avg <0.15:
            risk = 30.
        elif 0.15 <= avg:
            risk = 10.
        else:
            risk =100.
            
        return risk
    
    @__fault_tolerant
    def get_feature_4(cls, trading_variety_info):
        '''
        交易品种风险
        '''
        obj = json.loads(trading_variety_info)
        data = [each_line.get(u'交易品种', '')
                     for each_line in obj]
        
        for each_breed_name in data:         
            if (u'原油' in each_breed_name  or 
                        u'石油' in each_breed_name):
                risk = 100.
                break
            else:
                continue
        else:
            risk = 10.
        
        return risk
    
    @__fault_tolerant
    def get_feature_5(cls, legal_opinion):
        '''
        违规会员单位风险，必须在step_two中计算
        '''
        pass

    @__fault_tolerant
    def get_feature_6(cls, legal_opinion):
        '''
        高风险会员单位风险，必须在step_two中计算
        '''
        pass
    
        
class SparkUdf(ExFeatureConstruction):
    '''
    定义Spark-udf
    '''
    @classmethod
    def define_spark_udf(cls, func_num, return_type):
        func_name = eval('cls.get_feature_{func_num}'.format(func_num=func_num))
        return fun.udf(func_name, return_type)    


def spark_data_flow(exchange_version):
    exchange_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        trading_variety_info
        FROM
        dw.qyxg_exchange
        WHERE
        dt='{version}'
        '''.format(version=exchange_version)
    ).dropDuplicates(['company_name'])
    udf_return_type = tp.FloatType()
    tid_df = exchange_df.select(
        'bbd_qyxx_id',
        'company_name',
        SparkUdf.define_spark_udf(
            1, udf_return_type)('trading_variety_info').alias('ex_feature_1'),
        SparkUdf.define_spark_udf(
            2, udf_return_type)('trading_variety_info').alias('ex_feature_2'),
        SparkUdf.define_spark_udf(
            3, udf_return_type)('trading_variety_info').alias('ex_feature_3'),
        SparkUdf.define_spark_udf(
            4, udf_return_type)('trading_variety_info').alias('ex_feature_4'),
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
    #输入参数
    EXCHANGE_VERSION = '20170416'
    #中间结果版本
    RELATION_VERSION = '20170403'    
    
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_one/prd/"
    
    spark = get_spark_session()
    
    run(
        exchange_version=EXCHANGE_VERSION,
        relation_version=RELATION_VERSION
    )
    