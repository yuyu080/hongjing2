# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
ex_risk_score.py {version}
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
    constant = -2.156427
    weigth = np.array([
            0.6804028, -0.05419174, -0.6555351, 
            0.5309464, 0.1667864, -1.066347 ,
            4.5103E-07, 0, 0, 
            0.2523343, 0.06886173, -0.2928553, 
            0, 0, 0.208266,
            0.1367165, 0, -0.05860619,
            0.03441581, -0.1052271, 0.05509799,
            -0.00939167, 0, -0.6195755,
            0.09122186, -0.06998507, 0.2257222,
            0, -0.003179704, -0.0823541,
            0, -0.05680611, -0.08454077,
            -0.008233112, -0.03103419, -0.00078853,
            0.1797243
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
        
    return zip(bbd_qyxx_ids, company_names, ypred, data_set)

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
            score = sigmoid(raw_prob*100) * 30. / sigmoid(quantile_two*100)
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
    
    return (row[0], row[1], row[2], round(result, 1), row[3])


def get_first_grade_indexes(row):
    '''计算一级指标'''
    vector = np.nan_to_num(row[4])
    F1_weight = [0.3542, -0.0282, -0.3412, 
                 0.2764]
    F2_weight = [0.1353, -0.8647, 0.0000, 
                 0.0000, 0.0000, 0.2480]
    F3_weight = [0.0677, -0.2878, 0.0000, 
                 0.0000, 0.2047, 0.1343,
                 0.0000, -0.0576, 0.0284]
    F4_weight = [-0.0869, 0.0455, -0.0078,
                 0.0000, -0.5118, 0.0754,
                 -0.0578, 0.1864, 0.0000,
                 -0.0071]
    F5_weight = [-0.1844, 0.0000, -0.1272,
                 -0.1893, -0.0184, -0.0695,
                 -0.0018, 0.4024]
    
    #交易场所行业风险
    ex_trading_exchange_risk = sigmoid(np.dot(
            F1_weight, 
            vector[0:4]
    )) * 100.
    
    #综合实力风险
    ex_company_risk = sigmoid(np.dot(
            F2_weight, 
            vector[4:10]
    )) * 100.
    
    #企业行为风险
    ex_trading_risk = sigmoid(np.dot(
            F3_weight, 
            vector[10:19]
    )) * 100.
    
    #静态关联方风险
    ex_static_relationship_risk = sigmoid(np.dot(
            F4_weight,
            vector[19:29]
    )) * 100.

    #动态关联方风险
    ex_dynamic_relationship_risk = sigmoid(np.dot(
            F5_weight,
            vector[29:]
    )) * 100.

    return dict(
        bbd_qyxx_id=row[0],
        company_name=row[1],
        total_score=row[3],
        ex_trading_exchange_risk=round(ex_trading_exchange_risk, 1),
        ex_company_risk=round(ex_company_risk, 1),
        ex_trading_risk=round(ex_trading_risk, 1),
        ex_static_relationship_risk=round(ex_static_relationship_risk, 1),
        ex_dynamic_relationship_risk=round(ex_dynamic_relationship_risk, 1)
    )


def spark_data_flow(input_version):
    input_df = spark.read.parquet(
        "{path}/ex_feature_preprocessing/"
        "{version}".format(path=IN_PATH,
                           version=RELATION_VERSION))

    #step_one 计算判黑概率
    raw_rdd = input_df.rdd.coalesce(
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
    #取20%与50%的分位点
    top_twenty_index = int(len(score_distribution) * 0.2) - 1
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
         "ex_feature_risk_score/{version}").format(path=OUT_PATH, 
                                                  version=RELATION_VERSION))    
    prd_df.repartition(10).saveAsTextFile(
        ("{path}/"
         "ex_feature_risk_score/{version}").format(path=OUT_PATH, 
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
        .appName("hgongjing2_two_prd_ex_risk_score") \
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