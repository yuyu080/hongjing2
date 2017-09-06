# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
pe_risk_score.py {version}
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

def get_label_probability(iter_data):
    '''
    LR
    '''
    company_names = []
    bbd_qyxx_ids = []
    ypred = []
    data_set = []
    constant = -7.0745687308
    weigth = np.array([
        0.304047088,0,0,
        -0.997268829,0.064580886,0.063201681,
        0.218520266,-0.561999604,-0.303174256,
        0.019089207,0.139312884,0.599042852,
        0.053229527,-0.349865399,-0.563913097,
        0.567118769,-0.071488942,-0.165209758,
        0.113753871,0.099316849,0.051643276,
        0,-0.517370946,-0.127178548,
        0,0.21599605,0,
        0.060278979,-0.069630306,0.195803273,
        0.066959358,0.102127333,-0.972645934,
        0,-0.419776946,0.248032831,
        -0.038707299,-0.021131121,0.028375666,
        0.09138559,0,0.114698087,
        0.007362335,0.008146126,0,
        0.02833548
    ])
    
    for each_row in iter_data:
        company_names.append(each_row['company_name'])
        bbd_qyxx_ids.append(each_row['bbd_qyxx_id'])
        ypred.append(
            #LR
            sigmoid(
                np.nan_to_num(
                    each_row['scaledFeatures']
                ).dot(weigth) + constant
            )
        )
        data_set.append(each_row['scaledFeatures'].values)
        
    return zip(bbd_qyxx_ids, company_names, 
                      ypred, data_set)



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
        try:
            score = sigmoid(raw_prob*3000) * 30. / sigmoid(quantile_two*3000)
        except:
            score = 0.
    
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
    
    return (row[0], row[1], row[2],
                round(result, 1), row[3])



def get_first_grade_indexes(row):
    '''计算一级指标'''
    vector = np.nan_to_num(row[4])
    F1_weight = [
        0.0915, 0.0000, 0.0000,
        -0.3001, 0.0194, 0.0190,
        0.0658, -0.1691, -0.0912,
        0.0057, 0.0419, 0.1802,
        0.0160]
    F2_weight = [
        -0.203694819, -0.124799257, 0.330181708,
        -0.041621513, -0.096186624]
    F3_weight = [
        0.1011, 0.0883, 0.0459,
        0.0000, -0.4598, -0.1130,
        0.0000, 0.1920, 0.0000]
    F4_weight = [
        0.0277, -0.0320, 0.0901,
        0.0308, 0.0470, -0.4474,
        0.0000, -0.1931, 0.1141,
        -0.0178]
    F5_weight = [
        -0.0706, 0.0948, 0.3052,
        0.0000, 0.3830, 0.0246,
        0.0272, 0.0000, 0.0946]
    
    #私募基金行业风险
    pe_trading_exchange_risk = sigmoid(np.dot(
            F1_weight, 
            vector[0:13]
    )) * 100.
    
    #综合实力风险
    pe_company_risk = sigmoid(np.dot(
            F2_weight, 
            vector[13:18]
    )) * 100.
    
    #企业行为风险
    pe_trading_risk = sigmoid(np.dot(
            F3_weight, 
            vector[18:27]
    )) * 100.
    
    #静态关联方风险
    pe_static_relationship_risk = sigmoid(np.dot(
            F4_weight,
            vector[27:37]
    )) * 100.

    #动态关联方风险
    pe_dynamic_relationship_risk = sigmoid(np.dot(
            F5_weight,
            vector[37:]
    )) * 100.

    return dict(
            bbd_qyxx_id=row[0],
            company_name=row[1],
            total_score=row[3],
            pe_trading_exchange_risk=round(pe_trading_exchange_risk, 1),
            pe_company_risk=round(pe_company_risk, 1),
            pe_trading_risk=round(pe_trading_risk, 1),
            pe_static_relationship_risk=round(pe_static_relationship_risk, 1),
            pe_dynamic_relationship_risk=round(pe_dynamic_relationship_risk, 1)
        )


def spark_data_flow(input_version):
    input_df = spark.read.parquet(
        "{path}/pe_feature_preprocessing/"
        "{version}".format(path=IN_PATH,
                           version=RELATION_VERSION))    
    
    raw_rdd = input_df.rdd.coalesce(
            100
    ).mapPartitions(
            get_label_probability
    )
    
    #step_two 将判黑概率转换成分数
    #计算分位点
    score_distribution = raw_rdd.map(
        lambda r: r[2]
    ).collect()
    score_distribution.sort(reverse=True)
    #取5%与50%的分位点
    top_twenty_index = int(len(score_distribution) * 0.05) - 1
    top_fifty_index = int(len(score_distribution) * 0.5) - 1
    #得到分位点
    Y_1 = score_distribution[top_twenty_index]
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

    spark = SparkSession \
        .builder \
        .appName("hgongjing2_two_prd_pe_risk_score") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark
    
if __name__ == '__main__':
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")

    IN_PATH = conf.get('feature_preprocessing', 'OUT_PATH')
    OUT_PATH = conf.get('risk_score', 'OUT_PATH')
    
    #中间结果版本
    RELATION_VERSION = sys.argv[1] 
    
    spark = get_spark_session()
    
    run()