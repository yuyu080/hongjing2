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

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def sigmoid(x):
    return 1. / (1 + math.exp(-x))

def get_first_grade_indexes(row):
    '''计算一级指标'''
    vector = row[4]
    F1_weight = [78.995978788237665, 19.643289163698, 1.3607320480643235]
    F2_weight = [26.060202553046778, 32.455344310155851, 
                 35.422074212291704, 6.062378924505663]
    F3_weight = [8.5671628439652956, 67.099469406473432, 24.33336774956128]
    F4_weight = [5.4992316645363379, 0.73619670729601905, 
                 16.465178242190262, 6.8112990078436821, 
                 27.336531281175152, 5.9500466959961198, 
                 0.94871599993642608, 35.054955599386361, 1.1978448016396601]
    F5_weight = [2.1642169432775109, 7.2205710674551362, 
                 2.7554815418792837, 1.8386921453073, 
                 4.9583378078768945, 0.24537414679201996,
                 0.14077225761444445, 75.358040614215426, 5.3185134755819945]
    
    #综合实力风险
    GM_company_strength_risk = np.dot(
        F1_weight, map(sigmoid,[vector[1], vector[2], vector[4]]))
    #经营行为风险
    GM_behavior_risk = np.dot(F2_weight, map(
        sigmoid, [vector[5], vector[6], vector[7], vector[8]]))
    #企业诚信风险
    GM_credit_risk = np.dot(F3_weight, map(
        sigmoid, [vector[9], vector[10], vector[12]]))
    #静态关联方风险
    GM_static_relationship_risk = np.dot(F4_weight, map(
        sigmoid, [vector[14], vector[15], vector[16],
                  vector[17], vector[19], vector[20],
                  vector[21], row[4][22], vector[23]]))
    #动态关联方风险
    GM_dynamic_relationship_risk = np.dot(F5_weight, map(
        sigmoid, [vector[24], vector[27], vector[28],
                  vector[29], vector[31], vector[32],
                  vector[33], vector[36], vector[38]]))
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
    
    #第二次变换，真是SB
    #result = score
    if 50 < score <= 51:
        result = (sigmoid(score / 100.) * 100 -62) * 15 / 10. + 50
    elif 51 < score <= 90:
        result = (sigmoid(score / 100.) * 100 -62) * 25 / 10. + 65
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
    MODEL_FILE = ("/data5/antifraud/Hongjing2/data/inputdata/model/"
                  "GM_release_LR.model")
    
    IN_PATH = "/user/antifraud/hongjing2/dataflow/step_two/tid"
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_two/prd"
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    spark = get_spark_session()
    
    run()
    