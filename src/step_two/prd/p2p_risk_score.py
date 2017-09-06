# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
p2p_risk_score.py {version}
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
    platform_names = []
    platform_states = []
    ypred = []
    data_set = []
    constant = -0.70911
    weigth = np.array([
        0,0,-1.964171,
        0.001087,0,0.040929,
        -2.292425,0.155148,0,
        0,0,-0.086621,
        0.042553,-0.422093,0,
        0,-0.349865399,-0.563913097,
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
        platform_names.append(each_row['platform_name'])
        platform_states.append(each_row['platform_state'])
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
                      platform_names, platform_states,
                      ypred, data_set)



def change_prob_score(row, quantile_one, quantile_two):
    '''根据分位点，将判黑概率值转化成分'''
    raw_prob = row[4]
    raw_prob_changed = sigmoid(row[4])
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
            score = sigmoid(raw_prob*500) * 30. / sigmoid(quantile_two*500)
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
                row[3], row[4],
                round(result, 1), row[5])



def get_first_grade_indexes(row):
    '''计算一级指标'''
    vector = np.nan_to_num(row[6])
    F1_weight = [0, 0, -0.392439641, 0.000217182,
                 0, 0.008177578, -0.458024502, 
                 0.030998434, 0, 0, 
                 0, -0.0173068, 0.008502052, 
                 -0.084333811, 0, 0]
    F2_weight = [-0.203694819, -0.124799257, 0.330181708,
                 -0.041621513, -0.096186624]
    F3_weight = [0.10109123, 0.088261281, 0.045894546,
                 0, -0.459779213, -0.113021524,
                 0, 0.191952205]
    F4_weight = [0, 0.027727703, -0.032029216, 
                 0.090067467, 0.030800607, 0.04697751,
                 -0.447407001, 0, -0.193093024, 
                 0.114092519, -0.017804954]
    F5_weight = [-0.070570117, 0.094764214, 0.305194021,
                 0, 0.383049126, 0.024587472,
                 0.027205043, 0, 0.094630008]
    
    #网络借贷行业风险
    p2p_trading_exchange_risk = sigmoid(np.dot(
            F1_weight, 
            vector[0:16]
    )) * 100.
    
    #综合实力风险
    p2p_company_risk = sigmoid(np.dot(
            F2_weight, 
            vector[16:21]
    )) * 100.
    
    #企业行为风险
    p2p_trading_risk = sigmoid(np.dot(
            F3_weight, 
            vector[21:29]
    )) * 100.
    
    #静态关联方风险
    p2p_static_relationship_risk = sigmoid(np.dot(
            F4_weight,
            vector[29:40]
    )) * 100.

    #动态关联方风险
    p2p_dynamic_relationship_risk = sigmoid(np.dot(
            F5_weight,
            vector[40:]
    )) * 100.

    return dict(
            bbd_qyxx_id=row[0],
            company_name=row[1],
            platform_name=row[2], 
            platform_state=row[3],
            total_score=row[5],
            p2p_trading_exchange_risk=round(p2p_trading_exchange_risk, 
                                            1),
            p2p_company_risk=round(p2p_company_risk, 1),
            p2p_trading_risk=round(p2p_trading_risk, 1),
            p2p_static_relationship_risk=round(p2p_static_relationship_risk, 
                                               1),
            p2p_dynamic_relationship_risk=round(p2p_dynamic_relationship_risk, 
                                                1)
        )


def spark_data_flow(input_version):
    input_df = spark.read.parquet(
        "{path}/p2p_feature_preprocessing/"
        "{version}".format(path=IN_PATH,
                           version=RELATION_VERSION))    

    #step_one 计算判黑概率    
    raw_rdd = input_df.rdd.coalesce(
        100
    ).mapPartitions(
        get_label_probability
    )
    
    #step_two 将判黑概率转换成分数
    #计算分位点
    score_distribution = raw_rdd.map(
        lambda r: r[4]
    ).collect()
    score_distribution.sort(reverse=True)
    #取12%与50%的分位点
    top_twenty_index = int(len(score_distribution) * 0.12) - 1
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
         "p2p_feature_risk_score/{version}").format(path=OUT_PATH, 
                                                    version=RELATION_VERSION))    
    prd_df.repartition(10).saveAsTextFile(
        ("{path}/"
         "p2p_feature_risk_score/{version}").format(path=OUT_PATH, 
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
        .appName("hgongjing2_two_prd_p2p_risk_score") \
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