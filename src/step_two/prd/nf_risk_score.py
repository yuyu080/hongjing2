# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
nf_risk_score.py
'''

import sys
import os
import json
import math
import numpy as np

import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def sigmoid(x):
    return 1. / (1 + math.exp(-x))

def get_first_grade_indexes(row):
    '''计算一级指标'''
    vector = row[4]
    F1_weight = [-0.79, 0.196, -0.014]
    F2_weight = [0.261, 0.325, -0.354, -0.061]
    F3_weight = [-0.086, -0.671, 0.243]
    F4_weight = [0.055, -0.007, 0.165,
                 0.068, -0.273, -0.06,
                 -0.009, 0.351, -0.012]
    F5_weight = [-0.022, -0.072, -0.028,
                 -0.018, -0.05, 0.002,
                 0.001, -0.754, -0.053]
    
    #综合实力风险
    GM_company_strength_risk = sigmoid(np.dot(F1_weight, 
                                              [vector[1], vector[2], 
                                               vector[4]])) * 100.
    #经营行为风险
    GM_behavior_risk = sigmoid(np.dot(F2_weight, 
                                      [vector[5], vector[6], 
                                       vector[7], vector[8]])) * 100.
    #企业诚信风险
    GM_credit_risk = sigmoid(np.dot(F3_weight, 
                                    [vector[9], vector[10], 
                                     vector[12]])) * 100.
    #静态关联方风险
    GM_static_relationship_risk = sigmoid(np.dot(
        F4_weight,[vector[14], vector[15], vector[16],
                   vector[17], vector[19], vector[20],
                   vector[21], row[4][22], vector[23]])) * 100.

    #动态关联方风险
    GM_dynamic_relationship_risk = sigmoid(np.dot(
        F5_weight,[vector[24], vector[27], vector[28],
                   vector[29], vector[31], vector[32],
                   vector[33], vector[36], vector[38]])) * 100.

    return dict(
            bbd_qyxx_id=row[0],
            company_name=row[1],
            total_score=row[3],
            GM_company_strength_risk=round(GM_company_strength_risk, 1),
            GM_behavior_risk=round(GM_behavior_risk, 1),
            GM_credit_risk=round(GM_credit_risk, 1),
            GM_static_relationship_risk=round(GM_static_relationship_risk, 1),
            GM_dynamic_relationship_risk=round(GM_dynamic_relationship_risk, 1)
        )


def change_prob_score(row, quantile_one, quantile_two):
    '''根据分位点，将判黑概率值转化成分'''
    raw_prob = row[2]
    raw_prob_changed = sigmoid(row[2])
    quantile_one_changed = sigmoid(quantile_one)
    quantile_two_changed = sigmoid(quantile_two)

    #第一次变换
    if raw_prob >= quantile_one:
        score = (
            (raw_prob_changed - quantile_one_changed) * 40. / 
            (0.75 - quantile_one_changed) + 
            50        
        )
    elif quantile_two <= raw_prob < quantile_one:
        score = ((raw_prob_changed - quantile_two_changed) * 50. / 
                 (quantile_one_changed - quantile_two_changed) + 
                 (quantile_one_changed - raw_prob_changed) * 30. / 
                 (quantile_one_changed - quantile_two_changed))
    elif raw_prob < quantile_two:
        score = sigmoid(raw_prob*50000) * 30. / sigmoid(quantile_two*50000) 
    
    #第二次变换
    #result = score
    if 50 < score <= 51:
        #result = (sigmoid(score / 100.) * 100 -62) * 15 / 10. + 50
        result = sigmoid(score - 50) * 100.
    elif 51 < score <= 90:
        #result = (sigmoid(score / 100.) * 100 -62) * 25 / 10. + 65
        result = (90 - sigmoid(1) * 100.) * score / 39 + 51
    else:
        result = score
    
    return (row[0], row[1], row[2], round(result, 1), row[3])
    
def get_label_probability(iter_data):
    '''计算判黑概率'''

    from sklearn.externals import joblib
    
    company_names = []
    bbd_qyxx_ids = []
    data_set = []
    
    for each_row in iter_data:
        data = each_row['scaledFeatures'].values
        company_names.append(each_row['company_name'])
        bbd_qyxx_ids.append(each_row['bbd_qyxx_id'])
        data_set.append(data)
        
    lr = joblib.load("GM_release_LR.model")
    raw_prob = lr.predict_proba(data_set)
    
    return zip(bbd_qyxx_ids, company_names, raw_prob[:, 1], data_set)


def spark_data_flow():
    input_df = spark.read.parquet(
        "{path}/nf_feature_preprocessing/"
        "{version}".format(path=IN_PATH,
                           version=RELATION_VERSION))
    #step_one 计算判黑概率
    raw_rdd = input_df.rdd.repartition(
        100
    ).mapPartitions(
        get_label_probability
    ).cache()
    
    
    #step_two 将判黑概率转换成分数
    #计算分位点
    score_distribution = raw_rdd.map(
        lambda r: r[2]
    ).collect()
    score_distribution.sort(reverse=True)
    #取5%与50%的分位点
    top_five_index = int(len(score_distribution) * 0.05) - 1
    top_fifty_index = int(len(score_distribution) * 0.5) - 1
    #得到分位点
    Y_1 = score_distribution[top_five_index]
    Y_2 = score_distribution[top_fifty_index]
    #得到结果
    tid_rdd = raw_rdd.map(
        lambda r: change_prob_score(r, Y_1, Y_2)
    ).cache()
    
    #step_three 计算一级指标的得分
    prd_rdd = tid_rdd.map(
        get_first_grade_indexes
    ).map(
        lambda r: json.dumps(r, ensure_ascii=False)
    )
    
    return prd_rdd
    
def run():
    prd_rdd = spark_data_flow()
    
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "nf_feature_risk_score/{version}").format(path=OUT_PATH, 
                                                   version=RELATION_VERSION))
    
    codec = "org.apache.hadoop.io.compress.GzipCodec"
    prd_rdd.coalesce(
        10
    ).saveAsTextFile(
        ("{path}/"
         "nf_feature_risk_score/{version}").format(path=OUT_PATH, 
                                                   version=RELATION_VERSION),
        codec
    )

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
    conf.set("spark.files", MODEL_FILE)

    spark = SparkSession \
        .builder \
        .appName("hgongjing2_two_prd_nf_risk_score") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark

if __name__ == '__main__':
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    MODEL_FILE = ("/data5/antifraud/Hongjing2/data/inputdata/model/"
                  "GM_release_LR.model")
    
    IN_PATH = conf.get('nf_feature_preprocessing', 'OUT_PATH')
    OUT_PATH = conf.get('risk_score', 'OUT_PATH')
    
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    spark = get_spark_session()
    
    run()
    