# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
pe_company_feature.py {version}

'''
import sys
import json
import os
import math

import configparser
from pyspark.sql import types as tp
from pyspark.sql import functions as fun
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


class PeFeatureConstruction(object):
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
    def get_feature_1(cls, is_filed):
        '''
        私募基金管理人是否备案
        '''
        return 0. if is_filed else 100.

    @__fault_tolerant
    def get_feature_2(cls, legal_opinion):
        '''
        私募整改合规风险
        '''
        if legal_opinion is None:
            risk = 100
        elif u'要求提交' in legal_opinion:
            risk = 100
        elif u'办结' in legal_opinion:
            risk = 50
        else:
            risk = 10
            
        return float(risk)
    
    @__fault_tolerant
    def get_feature_3(cls, ifcareer_qualification, executive_info):
        '''
        高管从业资格风险
        '''
        def has_qualification(info):
            if u'是' in info:
                return 1
            else:
                return 0
        if ifcareer_qualification == u'是':
            risk = 10
        else:
            if executive_info is not None:
                obj = json.loads(executive_info)
                requirements = [
                    has_qualification(
                        each_manager_info.get(u'是否具有基金从业资格', ''))
                    for each_manager_info in obj]
                
                if sum(requirements) == 0:
                    risk = 100
                elif sum(requirements) == len(requirements):
                    risk = 10
                else: 
                    risk = 50
            else:
                    risk = 100
                
        return float(risk)

    @__fault_tolerant
    def get_feature_4(cls, is_vip, vip_type):        
        '''
        会员资质认定
        '''
        if is_vip is None and vip_type is None:
            risk = 100
        else: 
            if u'是' in is_vip: 
                if u'普通会员' in vip_type:
                    risk = 10
                elif u'观察会员' in vip_type:
                    risk = 50
                else:
                    risk =100
            else:
                risk = 100
           
        return float(risk)
        
    @__fault_tolerant
    def get_feature_5(cls, managed_fund_type, 
                           application_othertype):
        '''
        基金管理类别风险
        '''
        risk_distribution = {
            u'证券投资基金': 20,
            u'股权投资基金': 50,
            u'创业投资基金': 70,
            u'其他投资基金': 70
        }
        fund_types = managed_fund_type + application_othertype
        
        score_list = [
            v 
            for k, v in risk_distribution.iteritems() 
            if k in fund_types]
        if score_list:
            risk = max(score_list)
        else:
            risk = 0.
        
        return float(risk)
        
    @__fault_tolerant
    def get_feature_6(cls, employees):
        '''
        人员规模风险
        '''
        if employees is None:
            risk = 100
        else:
            try:
                recruit_num = int(employees)
                if recruit_num < 10:
                    risk = 80
                elif 10 <= recruit_num < 50:
                    risk = 50
                elif 50 <= recruit_num:
                    risk = 30
                else:
                    risk = 10
            except:
                risk = 100

        return float(risk)
        
    @__fault_tolerant
    def get_feature_7(cls, abnormal_close):
        '''
        基金异常清盘风险
        '''
        if abnormal_close == u'否':
            risk = 10
        else:
            risk = 100
        
        return float(risk)

    @__fault_tolerant
    def get_feature_8(cls, interim_after_fund):
        '''
        暂行办法实施后基金发行异常风险
        '''
        if interim_after_fund is not None: 
            fund_num  = len(set(json.loads(interim_after_fund)))
            if fund_num > 0:
                risk = 10
            else:
                risk = 100
        else:
            risk = 100
            
        return float(risk)
        
    @__fault_tolerant    
    def get_feature_9(cls, interim_before_fund, interim_after_fund):
        '''
        基金规模风险
        '''
        interim_after_fund = json.loads(interim_after_fund)
        interim_before_fund = json.loads(interim_before_fund)
        
        if interim_after_fund or interim_before_fund:
            fund_num = len(set(
                interim_after_fund + interim_before_fund))
            risk = (1 - math.exp(-fund_num)) * 100
        else:
            risk = 100.
        
        return float(risk)
    
    @__fault_tolerant
    def get_feature_10(cls, integrity_info):
        '''
        机构诚信风险
        '''
        if integrity_info is not None and integrity_info != 'NULL':
            risk = 100
        else:
            risk = 10
            
        return dict(
            risk=str(risk),
            info=integrity_info.replace('\t', '') \
                               .replace('\r', '') \
                               .replace('rn', '')
        )
    
    
class SparkUdf(PeFeatureConstruction):
    '''
    定义Spark-udf
    '''
    @classmethod
    def define_spark_udf(cls, func_num, return_type):
        func_name = eval('cls.get_feature_{func_num}'.format(func_num=func_num))
        return fun.udf(func_name, return_type)    
    
    
def spark_data_flow(smjj_version):
    #私募信息
    smjj_df = spark.sql(
        '''
        SELECT
        *
        FROM
        dw.qyxg_jijin_simu
        WHERE
        dt = '{version}'
        '''.format(
            version=smjj_version
        )
    )
    tid_df = smjj_df.select(
        'bbd_qyxx_id',
        smjj_df.fund_manager_chinese.alias('company_name'),
        smjj_df.fund_manager_chinese.isNotNull().alias('is_filed'),
        'legal_opinion',
        'executive_info',
        'is_vip', 
        'vip_type',
        'managed_fund_type',
        'employees',
        'abnormal_close',
        'interim_after_fund',
        'interim_before_fund',
        'integrity_info',
        'ifcareer_qualification',
        'application_othertype'
    ).dropDuplicates(['company_name'])

    udf_return_type = tp.FloatType()
    prd_df = tid_df.select(
        'bbd_qyxx_id',
        'company_name',
        SparkUdf.define_spark_udf(
            1, udf_return_type)('is_filed').alias('pe_feature_1'),
        SparkUdf.define_spark_udf(
            2, udf_return_type)('legal_opinion').alias('pe_feature_2'),
        SparkUdf.define_spark_udf(
            3, udf_return_type)('ifcareer_qualification', 
                                'executive_info').alias('pe_feature_3'),        
        SparkUdf.define_spark_udf(
            4, udf_return_type)('is_vip', 'vip_type').alias('pe_feature_4'),        
        SparkUdf.define_spark_udf(
            5, udf_return_type)('managed_fund_type',
                                'application_othertype').alias('pe_feature_5'),        
        SparkUdf.define_spark_udf(
            6, udf_return_type)('employees').alias('pe_feature_6'),        
        SparkUdf.define_spark_udf(
            7, udf_return_type)('abnormal_close').alias('pe_feature_7'),         
        SparkUdf.define_spark_udf(
            8, udf_return_type)('interim_after_fund').alias('pe_feature_8'),         
        SparkUdf.define_spark_udf(
            9, udf_return_type)('interim_before_fund', 
                                'interim_after_fund').alias('pe_feature_9'),         
        SparkUdf.define_spark_udf(
            10, 
            tp.MapType(tp.StringType(), tp.StringType())
        )('integrity_info').alias('pe_feature_10')
    )
    
    return prd_df
    
def run(smjj_version, relation_version):
    '''
    格式化输出
    '''
    pd_df = spark_data_flow(smjj_version)
    os.system(
        ("hadoop fs -rmr "
         "{path}/"
         "pe_feature_distribution/{version}").format(path=OUT_PATH, 
                                                     version=relation_version))
    pd_df.repartition(10).write.json(
        ("{path}/"
         "pe_feature_distribution/{version}").format(path=OUT_PATH, 
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
        .appName("hongjing2_one_prd_pe") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()    
    
    return spark
    
    
if __name__ == '__main__':  
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #输入参数
    SMJJ_VERSION = conf.get('pe_company_feature', 'SMJJ_VERSION')
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    OUT_PATH = conf.get('pe_company_feature', 'OUT_PATH')

    spark = get_spark_session()
    
    run(
        smjj_version=SMJJ_VERSION,
        relation_version=RELATION_VERSION
    )
    