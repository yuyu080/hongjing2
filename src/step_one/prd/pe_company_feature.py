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
            except Exception:
                return {'error': 'error'}
        return wappen    

    @__fault_tolerant
    def get_feature_1(cls, managed_fund_type):
        '''
        基金管理类型
        '''
        if not managed_fund_type:
            risk = 70
        else:
            if u'证券投资基金' in managed_fund_type:
                risk = 20
            elif u'股权投资基金' in managed_fund_type:
                risk = 50
            else:
                risk = 70
        
        return float(risk)
    
    @__fault_tolerant
    def get_feature_2(cls, pic_millon):
        '''
        实缴资本
        '''
        try:
            risk = pic_millon.replace(',', '')
            return float(risk)
        except:
            return 0.

    @__fault_tolerant
    def get_feature_3(cls, regcap_paidpro):
        '''
        认缴比例
        '''
        try:
            risk = regcap_paidpro.replace('%', '')
            return round(float(risk) / 100., 3)
        except:
            return 0.
        
    @__fault_tolerant
    def get_feature_4(cls, law_firm):
        '''
        是否聘请律师事务所
        '''
        if law_firm and law_firm != 'NULL' and law_firm != 'null':
            return 1.
        else:
            return 0.

    @__fault_tolerant
    def get_feature_5(cls, no_qualification):
        '''
        高管人员是否有证券投资基金从业资格
        '''
        if no_qualification == u'是':
            return 1.
        else:
            return 0.
        
    @__fault_tolerant
    def get_feature_6(cls, employees):
        '''
        员工人数
        '''
        try:
            if employees:
                return float(employees)
            else:
                return 0.
        except:
            return 0.
        
    @__fault_tolerant
    def get_feature_7(cls, entitled_way):
        '''
        资格取得方式
        '''
        if not entitled_way:
            return 0.
        else:
            if u'通过考试' in entitled_way:
                return 1.
            elif u'资格认定' in entitled_way:
                return 2.
            else:
                return 0.

    @__fault_tolerant
    def get_feature_8(cls, ifcareer_qualification):
        '''
        是否有从业资格
        '''
        if not ifcareer_qualification:
            return 0.
        else:
            if ifcareer_qualification == u'是':
                return 1.
            else:
                return 0.        
            
    @__fault_tolerant
    def get_feature_9(cls, vip_type):
        '''
        会员等级
        '''
        if not vip_type:
            return 0.
        else:
            if vip_type == u'普通会员':
                return 2.
            elif vip_type == u'观察会员':
                return 1.
            else:
                return 0.             
            
    @__fault_tolerant    
    def get_feature_10(cls, interim_after_fund):
        '''
        私募整改后发行基金数量
        '''
        if interim_after_fund: 
            fund_num = len(set(json.loads(interim_after_fund)))
        else:
            fund_num = 0.
        
        return float(fund_num)
            
    @__fault_tolerant    
    def get_feature_11(cls, interim_before_fund):
        '''
        私募整改前发行基金数量
        '''
        if interim_before_fund: 
            fund_num = len(set(json.loads(interim_before_fund)))
        else:
            fund_num = 0.
        
        return float(fund_num)
            
    @__fault_tolerant
    def get_feature_12(cls, integrity_info):
        '''
        是否提示为异常机构
        '''
        if integrity_info and u'异常机构' in integrity_info:
            risk = 1.
        else:
            risk = 0.
            
        return dict(
            risk=str(risk),
            info=integrity_info.replace(
                '\t', ''
            ) .replace(
                '\r', ''
            ).replace(
                'rn', ''
            )
        )
            
    @__fault_tolerant
    def get_feature_13(cls, integrity_info, special_message):
        '''
        是否有其他诚信提示，是否有其他特别信息
        '''
        if integrity_info and u'其他诚信信息' in integrity_info:
            risk = 1.
        else:
            risk = 0.
            
        return dict(
            risk=str(risk),
            special_info=special_message.replace(
                '\t', ''
            ) .replace(
                '\r', ''
            ).replace(
                'rn', ''
            )
        )
            
    @__fault_tolerant
    def get_feature_14(cls, legal_opinion):
        '''
        是否按要求提供法律意见书
        '''
        if (legal_opinion == 'NULL' or 
                legal_opinion == 'null' or not legal_opinion):
            risk = 0.
        else:
            risk = 1.
            
        return float(risk)

    
    
class SparkUdf(PeFeatureConstruction):
    '''
    定义Spark-udf
    '''
    @classmethod
    def define_spark_udf(cls, func_num, return_type):
        func_name = eval(
            'cls.get_feature_{func_num}'.format(func_num=func_num))
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
        'managed_fund_type',
        'pic_millon',
        'regcap_paidpro',
        'law_firm',
        'no_qualification',
        'employees',
        'entitled_way',
        'ifcareer_qualification',
        'vip_type',
        'interim_after_fund',
        'interim_before_fund',
        'integrity_info',
        'special_message',
        'legal_opinion'
    ).dropDuplicates(
        ['company_name']
    ).cache()
    
    udf_return_type = tp.FloatType()
    prd_df = tid_df.select(
        'bbd_qyxx_id',
        'company_name',
        SparkUdf.define_spark_udf(
            1, udf_return_type)('managed_fund_type').alias('pe_feature_1'),
        SparkUdf.define_spark_udf(
            2, udf_return_type)('pic_millon').alias('pe_feature_2'),
        SparkUdf.define_spark_udf(
            3, udf_return_type)('regcap_paidpro').alias('pe_feature_3'),
        SparkUdf.define_spark_udf(
            4, udf_return_type)('law_firm').alias('pe_feature_4'),
        SparkUdf.define_spark_udf(
            5, udf_return_type)('no_qualification').alias('pe_feature_5'),
        SparkUdf.define_spark_udf(
            6, udf_return_type)('employees').alias('pe_feature_6'),
        SparkUdf.define_spark_udf(
            7, udf_return_type)('entitled_way').alias('pe_feature_7'),
        SparkUdf.define_spark_udf(
            8, udf_return_type)('ifcareer_qualification').alias('pe_feature_8'),
        SparkUdf.define_spark_udf(
            9, udf_return_type)('vip_type').alias('pe_feature_9'),    
        SparkUdf.define_spark_udf(
            10, udf_return_type)('interim_after_fund').alias('pe_feature_10'), 
        SparkUdf.define_spark_udf(
            11, udf_return_type)('interim_before_fund').alias('pe_feature_11'), 
        SparkUdf.define_spark_udf(
            12, tp.MapType(tp.StringType(), tp.StringType())
        )('integrity_info').alias('pe_feature_12'),
        SparkUdf.define_spark_udf(
            13, tp.MapType(tp.StringType(), tp.StringType())
        )('integrity_info', 'special_message').alias('pe_feature_13'),
        SparkUdf.define_spark_udf(
            14, udf_return_type)('legal_opinion').alias('pe_feature_14')
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
    
    OUT_PATH = conf.get('common_company_feature', 'OUT_PATH')

    spark = get_spark_session()
    
    run(
        smjj_version=SMJJ_VERSION,
        relation_version=RELATION_VERSION
    )
    