# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
pe_risk_score.py
'''

import os
import json

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def get_label_probability(iter_data):
    '''
    调用P2P模型
    '''
    import xgboost as xgb
    
    company_names = []
    bbd_qyxx_ids = []
    data_set = []
    
    for each_row in iter_data:
        if (each_row['feature_25'] and each_row['feature_26'] and 
                  each_row['feature_27'] and each_row['feature_28']):
            data = [each_row['pe_feature_1'], each_row['pe_feature_2'],
                         each_row['pe_feature_3'], each_row['pe_feature_4'],
                         each_row['pe_feature_5'], each_row['pe_feature_6'],
                         each_row['pe_feature_7'], each_row['pe_feature_8'],
                         each_row['pe_feature_9'], int(each_row[
                                                     'pe_feature_10']['risk']),
                         each_row['feature_1']['r'], each_row['feature_2']['c'],
                         each_row['feature_3']['j'], each_row['feature_4']['k'],
                         each_row['feature_5']['l'], each_row['feature_6']['z'],
                         each_row['feature_7']['z'], each_row['feature_8']['y'],
                         each_row['feature_9']['n'], each_row['feature_10']['z'],
                         each_row['feature_11']['z'], each_row['feature_12']['z'],
                         each_row['feature_13']['z'], each_row['feature_14']['z'],
                         each_row['feature_15']['r'], each_row['feature_16']['r'],
                         each_row['feature_17']['r'], each_row['feature_18']['z'],
                         each_row['feature_19']['z'], each_row['feature_20']['g'],
                         each_row['feature_21']['y'], each_row['feature_22']['z'],
                         each_row['feature_23']['y'], each_row['feature_24']['z'],
                         each_row['feature_25']['p'], each_row['feature_26']['h'],
                         each_row['feature_27']['c'], each_row['feature_28']['k']]
        elif each_row['feature_1']:
            data = [each_row['pe_feature_1'], each_row['pe_feature_2'],
                         each_row['pe_feature_3'], each_row['pe_feature_4'],
                         each_row['pe_feature_5'], each_row['pe_feature_6'],
                         each_row['pe_feature_7'], each_row['pe_feature_8'],
                         each_row['pe_feature_9'], int(each_row[
                                                     'pe_feature_10']['risk']),
                         each_row['feature_1']['r'], each_row['feature_2']['c'],
                         each_row['feature_3']['j'], each_row['feature_4']['k'],
                         each_row['feature_5']['l'], each_row['feature_6']['z'],
                         each_row['feature_7']['z'], each_row['feature_8']['y'],
                         each_row['feature_9']['n'], each_row['feature_10']['z'],
                         each_row['feature_11']['z'], each_row['feature_12']['z'],
                         each_row['feature_13']['z'], each_row['feature_14']['z'],
                         each_row['feature_15']['r'], each_row['feature_16']['r'],
                         each_row['feature_17']['r'], each_row['feature_18']['z'],
                         each_row['feature_19']['z'], each_row['feature_20']['g'],
                         each_row['feature_21']['y'], each_row['feature_22']['z'],
                         each_row['feature_23']['y'], each_row['feature_24']['z'],
                         0, 0,
                         0, 0]            
        else:
            data = [each_row['pe_feature_1'], each_row['pe_feature_2'],
                         each_row['pe_feature_3'], each_row['pe_feature_4'],
                         each_row['pe_feature_5'], each_row['pe_feature_6'],
                         each_row['pe_feature_7'], each_row['pe_feature_8'],
                         each_row['pe_feature_9'], int(each_row[
                                                     'pe_feature_10']['risk']),
                         0 ,100,
                         0, 100,
                         100, 10,
                         10, 10,
                         100, 10,
                         10, 10,
                         10, 10,
                         10, 10,
                         10, 10,
                         10, 10,
                         10, 10,
                         10, 10,
                         0, 0,
                         0, 0]

        company_names.append(each_row['company_name'])
        bbd_qyxx_ids.append(each_row['bbd_qyxx_id'])
        data_set.append(data)
        
    dpred = xgb.DMatrix(data_set)
    bst = xgb.Booster()
    bst.load_model("PE_28feats_release.model")
    ypred  = bst.predict(dpred)

    return zip(bbd_qyxx_ids, company_names, ypred)

def change_score(src_score):
    '''
    对风险总值做一个非线性变换
    '''
    from math import sqrt
    if src_score <= 50:
        if src_score > 2:
            des_score = 20*sqrt(2) + src_score/2.3
        else:
            des_score = 20 * sqrt(src_score)
    else:
        des_score = src_score
    return des_score

def get_subdivision_index(row):
    '''
    根据权重分布对每个一级指标打分，ex为交易平台的权重
    '''   
    from risk_weight import risk_weight
    ex_weight = risk_weight['PE']
    total_score = round(change_score(row[2]*100.), 2)
    risk_distribution = {k: round(v*total_score/100., 2) 
                         for k, v in ex_weight.iteritems()}
    row_dict = dict(
        bbd_qyxx_id = row[0],
        company_name = row[1],
        total_score = total_score,
        **risk_distribution
    )
    return json.dumps(row_dict, ensure_ascii=False)

def spark_data_flow(input_version):
    feature_df = spark.read.parquet(
    "{path}/pe_feature_merge/{version}".format(path=IN_PATH,
                                               version=input_version))

    feature_rdd = feature_df.rdd \
        .coalesce(100) \
        .mapPartitions(get_label_probability) \
        .map(get_subdivision_index)
    
    return feature_rdd

def run():
    prd_df = spark_data_flow(RELATION_VERSION)
    
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "pe_feature_risk_score/{version}").format(path=OUT_PATH, 
                                                   version=RELATION_VERSION))
    prd_df.repartition(10).saveAsTextFile(
        ("{path}/"
         "pe_feature_risk_score/{version}").format(path=OUT_PATH, 
                                                   version=RELATION_VERSION))

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
    conf.set("spark.files", FILES)

    spark = SparkSession \
        .builder \
        .appName("hgongjing2_two_prd_pe_risk_score") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark
    
if __name__ == '__main__':
    MODEL_FILE = ("/data5/antifraud/Hongjing2/data/inputdata/model/"
                  "PE_28feats_release.model")
    WEIGHT_FILE = ("/data5/antifraud/Hongjing2/data/inputdata/weight/"
                  "risk_weight.py")
    FILES = ','.join([MODEL_FILE, 
                      WEIGHT_FILE])
    
    IN_PATH = "/user/antifraud/hongjing2/dataflow/step_two/raw"
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_two/prd"
    #中间结果版本
    RELATION_VERSION = '20170117' 
    
    spark = get_spark_session()
    
    run()