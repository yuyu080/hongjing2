# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
common_company_dynamic_feature_v2.py {version}

'''
import sys
import os
import json

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_dynamic_risk(data):
    '''
    计算动态风险
    '''
    old_data, new_data = data
    
    def get_result(my_denominator, my_numerator):
        '''特殊计算规则，嘻嘻'''
        if not denominator and  numerator:
            result = 65535.
        elif not denominator and  not numerator:
            result = 0.
        else:
            result = numerator * 1. / denominator 
        return result

    #3度及3度以下自然人节点变动率
    denominator = (old_data['feature_16']['y_1'] + 
                               old_data['feature_16']['y_2'] +
                               old_data['feature_16']['y_3'])
    numerator = (new_data['feature_16']['y_1'] + 
                               new_data['feature_16']['y_2'] +
                               new_data['feature_16']['y_3']) - denominator
    a_1 = get_result(denominator, numerator)
        
    #对外投资公司数量变动率
    denominator = old_data['feature_17']['x']
    numerator = new_data['feature_17']['x'] - denominator
    a_4 = get_result(denominator, numerator)
    
    #公司分支机构数量变动率
    denominator = old_data['feature_18']['d']
    numerator = new_data['feature_18']['d'] - denominator
    a_5 = get_result(denominator, numerator)
    
    #利益一致行动法人的数量变动率
    denominator = old_data['feature_22']['d']
    numerator = new_data['feature_22']['d'] - denominator
    a_6 = get_result(denominator, numerator)
    
    #法定代表人变更次数变动率
    denominator = old_data['feature_6']['c_1']
    numerator = new_data['feature_6']['c_1'] - denominator
    b_1 = get_result(denominator, numerator)
    
    #股东变更次数变动率
    denominator = old_data['feature_6']['c_2']
    numerator = new_data['feature_6']['c_2'] - denominator
    b_2 = get_result(denominator, numerator)
    
    #注册资本变更次数变动率
    denominator = old_data['feature_6']['c_3']
    numerator = new_data['feature_6']['c_3'] - denominator
    b_3 = get_result(denominator, numerator)
    
    #大专及大专以下或不限专业招聘人数变动率
    denominator = old_data['feature_7']['e_1']
    numerator = new_data['feature_7']['e_1'] - denominator
    c_1 = get_result(denominator, numerator)
    
    #3度及3度以下核心自然人（前3）控制节点总数变动率
    denominator = old_data['feature_1']['r_4']
    numerator = new_data['feature_1']['r_4'] - denominator
    d_2 = get_result(denominator, numerator)
    
    
    return dict(
        feature_26={
            'a_1': a_1,
            'a_4': a_4,
            'a_5': a_5,
            'a_6': a_6,
            'b_1': b_1,
            'b_2': b_2,
            'b_3': b_3,
            'c_1': c_1,
            'd_2': d_2
        }
    )
    
def spark_data_flow(tid_old_version, tid_new_version):
    '''
    dataflow
    '''
    #输入
    #算法设计直接用rdd
    tid_old_df = spark.read.json(
        ("{path}/"
         "common_static_feature_distribution_v2/"
         "{version}").format(path=IN_PATH,
                             version=tid_old_version))
    feature_old_rdd = tid_old_df.rdd.map(lambda r: (r.bbd_qyxx_id, r))
    tid_new_df = spark.read.json(
        ("{path}/"
         "common_static_feature_distribution_v2/"
         "{version}").format(path=IN_PATH,
                             version=tid_new_version))
    feature_new_rdd = tid_new_df.rdd.map(lambda r: (r.bbd_qyxx_id, r))
        
    #最终计算流程
    dynamic_risk_data = feature_old_rdd.join(
        feature_new_rdd
    ).mapValues(
        get_dynamic_risk
    ).map(
        lambda (k, v): dict({'bbd_qyxx_id': k}, **v)
    ).map(
        lambda data: json.dumps(data, ensure_ascii=False)
    )
    
    return dynamic_risk_data
    
def get_spark_session():
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "30g")
    conf.set("spark.executor.instances", 20)
    conf.set("spark.executor.cores", 7)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 1000)
    conf.set("spark.sql.shuffle.partitions", 1000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)
    
    spark = SparkSession \
        .builder \
        .appName("hgongjing2_one_prd_common_dynamic_v2") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark  

def run(tid_old_version, tid_new_version):
    '''
    格式化输出
    '''
    dynamic_risk_data = spark_data_flow(tid_old_version, tid_new_version)
    os.system(
        ("hadoop fs -rmr "
         "{path}/"
         "common_dynamic_feature_distribution_v2/"
         "{version}").format(path=OUT_PATH, 
                             version=tid_new_version))
    dynamic_risk_data.repartition(10).saveAsTextFile(
        ("{path}/"
         "common_dynamic_feature_distribution_v2/"
         "{version}").format(path=OUT_PATH, 
                             version=tid_new_version))

if __name__ == '__main__':  
    import configparser
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")

    #输入参数
    IN_PATH = conf.get('common_company_feature', 'OUT_PATH')
    OUT_PATH = conf.get('common_company_feature', 'OUT_PATH')
    
    spark = get_spark_session()

    run(tid_old_version=sys.argv[1], 
        tid_new_version=sys.argv[2])