# -*- coding: utf-8 -*-
# 重新写提交命令吧
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
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
import pandas as pd

# 测试数列或者单个数可不可以

from sklearn.externals import joblib

reload(sys)
sys.setdefaultencoding("utf-8")


def linear(data, weight):
    return np.dot(data, weight[1:]) + weight[0]

def sigmoid(x):
    return 1. / (1 + math.exp(-x))

def score_change(data, index):
    return math.pow(data, index) * 100

def logistic_count(data, weight, index):
    return score_change(sigmoid(linear(data, weight)), index)

def logistic_count_sub(data, weight, b, index):
    return score_change(sigmoid(np.dot(data, weight) + b/DIMENSION * len(weight)), index)

def get_label_probability(row):
    company_names = []
    bbd_qyxx_ids = []
    data_set = []
    for each_row in row:
        data = each_row['scaledFeatures'].values
        company_names.append(each_row['company_name'])
        bbd_qyxx_ids.append(each_row['bbd_qyxx_id'])
        data_set.append(data)
    return zip(bbd_qyxx_ids, company_names, data_set)

def runSQL(sqlstr):
    return spark.sql(sqlstr).na.replace(['null', 'NULL', 'NaN', 'nan'], ['', '', '', '']).fillna(0.)

def writeCSV(df, path, fileName):
    try:
        os.system("hadoop fs -rmr {path}{fileName}".format(path=path, fileName=fileName))
        df.write.csv(path="{path}{fileName}".format(path=path, fileName=fileName), header=True)
    except:
        print "Write CSV error."

def facotrMatrix(lists):
    '''
    change the factorlist to the factorMatrix
    :param lists: the factorlist
    :return: the factorMatrix
    '''
    result = []
    for i in range(0, len(lists[0]), 1):
        tem = []
        for j in range(0, len(lists), 1):
            tem.append(lists[j][i])
        result.append(tem)
    return result

def count_logistic(iter_data, weight, zhibiao, sk, b, regression_coefficient):
    company_names = []
    bbd_qyxx_ids = []
    score_all = []
    score_sub = []
    for each_row in iter_data:
        data = each_row['scaledFeatures'].values
        company_names.append(each_row['company_name'])
        bbd_qyxx_ids.append(each_row['bbd_qyxx_id'])
        score = 0
        for i in range(0, ITERATION, 1):
            score += logistic_count(data, weight[i], 0.2)
        Score_logistic = score/10
        score_all.append(Score_logistic)
        '''子模块风险计算'''
        score_submodule = []
        for i in range(0, 5, 1):
            data_sub = [data[x-1] for x in zhibiao[i]]
            weight_sub = [regression_coefficient[x] for x in zhibiao[i]]
            score_submodule.append(logistic_count_sub(data_sub, weight_sub, b, sk[i]))
        score_sub.append(score_submodule)
    # 转置处理
    score_sub = facotrMatrix(score_sub)
    return zip(bbd_qyxx_ids, company_names, score_all, score_sub[0], score_sub[1], score_sub[2], score_sub[3], score_sub[4])

def logistic_score():
    input_df = spark.read.parquet(
        "{path}/nf_feature_preprocessing/"
        "{version}".format(path=IN_PATH, version=RELATION_VERSION))
    # 有数据了再修改这个
    # raw_collect = input_df.rdd.map(lambda x: x['scaledFeatures'].values).collect()
    weight_df = spark.read.csv(WEIGHT_PATH, encoding="utf-8", header=True, sep='\t')
    weight_df.registerTempTable('weight')
    weight_df = spark.sql('''
    select cast(V1 as double) V1, cast(V2 as double) V2, cast(V3 as double) V3, cast(V4 as double) V4, cast(V5 as double) V5, cast(V6 as double) V6, cast(V7 as double) V7, cast(V8 as double) V8, cast(V9 as double) V9, cast(V10 as double) V10 from weight
    ''')
    # 把它存到数据库里面
    # weight_df.show()
    weight = weight_df.rdd.collect()
    # 计算每一行的均值，为平均回归系数(包括截距项，要加1)
    regression_coefficient = []
    for i in range(0, DIMENSION + 1, 1):
        regression_coefficient.append(sum(weight[i])/ITERATION)
    print regression_coefficient
    # 转置处理
    weight = facotrMatrix(weight)
    # weight[0]表示第一列，注意包括了截距项，因此做乘法的时候要去掉截距项再加上截距项

    zhibiao = [[1, 2, 33, 34, 35], [44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57], range(3, 26), [26, 27, 28, 29, 30, 31, 32, 36, 37, 38, 39, 40, 41, 42, 43], range(58, 62)]
    sk = [0.8, 0.5, 0.4, 0.5, 0.8]
    b = regression_coefficient[0]

    raw_rdd = input_df.rdd.mapPartitions(lambda x: count_logistic(x, weight, zhibiao, sk, b, regression_coefficient))
    raw_df = raw_rdd.toDF().withColumnRenamed(
        '_1', 'bbd_qyxx_id'
    ).withColumnRenamed(
        '_2', 'company_name'
    ).withColumnRenamed(
        '_3', 'Score_logistic'
    ).withColumnRenamed(
        '_4', 'GM_company_strength_risk'
    ).withColumnRenamed(
        '_5', 'GM_behavior_risk'
    ).withColumnRenamed(
        '_6', 'GM_credit_risk'
    ).withColumnRenamed(
        '_7', 'GM_static_relationship_risk'
    ).withColumnRenamed(
        '_8', 'GM_dynamic_relationship_risk'
    ).orderBy('bbd_qyxx_id')
    #     .map(
    #     lambda r: json.dumps(r, ensure_ascii=False)
    # )

    return raw_df

def count_gbdt(iter_data, gbdt):
    '''
    调用通用模型
    '''
    company_names = []
    bbd_qyxx_ids = []
    data_set = []

    for row in iter_data:
        data =[row['feature_1']['n'],
               row['feature_1']['r_3'],
               row['feature_1']['r_4'],
               row['feature_1']['r_i'],
               row['feature_1']['w'],
               row['feature_1']['x'],
               row['feature_1']['y'],
               row['feature_1']['z'],
               row['feature_15']['x_1'],
               row['feature_15']['x_2'],
               row['feature_15']['y_1'],
               row['feature_15']['y_2'],
               row['feature_16']['v_1'],
               row['feature_16']['v_2'],
               row['feature_16']['v_3'],
               row['feature_16']['w_1'],
               row['feature_16']['w_1'],
               row['feature_16']['w_3'],
               row['feature_16']['x_1'],
               row['feature_16']['x_2'],
               row['feature_16']['x_3'],
               row['feature_16']['y_1'],
               row['feature_16']['y_2'],
               row['feature_16']['y_3'],
               row['feature_17']['x'],
               row['feature_17']['y'],
               row['feature_18']['d'],
               row['feature_19']['x_1'],
               row['feature_19']['x_2'],
               row['feature_19']['x_3'],
               row['feature_2']['x'],
               row['feature_2']['x_1'],
               row['feature_2']['y'],
               row['feature_20']['x_1'],
               row['feature_20']['x_2'],
               row['feature_20']['x_3'],
               row['feature_20']['y_1'],
               row['feature_20']['y_2'],
               row['feature_20']['y_3'],
               row['feature_20']['z_1'],
               row['feature_20']['z_2'],
               row['feature_20']['z_3'],
               row['feature_22']['d'],
               row['feature_24']['w'],
               row['feature_24']['x_0'],
               row['feature_24']['x_1'],
               row['feature_24']['x_2'],
               row['feature_24']['x_3'],
               row['feature_4']['k_1'],
               row['feature_4']['k_2'],
               row['feature_5']['c_i'],
               row['feature_5']['p_i'],
               row['feature_6']['c_1'],
               row['feature_6']['c_2'],
               row['feature_6']['c_3'],
               row['feature_6']['c_4'],
               row['feature_6']['c_5'],
               row['feature_7']['e'],
               row['feature_7']['e_1'],
               row['feature_7']['e_2'],
               row['feature_7']['e_3'],
               row['feature_7']['e_4'],
               row['feature_7']['e_5'],
               row['feature_7']['e_6'],
               row['feature_8']['t_1'],
               row['feature_8']['t_2'],
               row['feature_8']['t_3'],
               row['feature_9']['n_1'],
               row['feature_9']['n_2'],
               row['feature_26']['a_1'],
               row['feature_26']['a_4'],
               row['feature_26']['a_5'],
               row['feature_26']['a_6'],
               row['feature_26']['b_1'],
               row['feature_26']['b_2'],
               row['feature_26']['b_3'],
               row['feature_26']['c_1'],
               row['feature_26']['d_2']]
        company_names.append(row['company_name'])
        bbd_qyxx_ids.append(row['bbd_qyxx_id'])
        data_set.append(data)
    # dpred = xgb.DMatrix(data_set)
    # ypred = bst.predict(dpred)
    prob = gbdt.predict_proba(pd.DataFrame(data_set))
    prob = facotrMatrix(prob)[1]
    gbdtS = []
    for list in prob:
        gbdtS.append(score_change(list, 0.2))

    return zip(bbd_qyxx_ids, company_names, gbdtS)


def GBDT_score():
    '''
    调用通用模型
    '''

    # 有数据了再修改这个
    feature_df = spark.read.parquet(
        "{path}/nf_feature_merge/{version}".format(path=IN_PATH_GBDT, version=RELATION_VERSION))

    gbdt = joblib.load(MODEL_FILE)
    feature_rdd = feature_df.rdd.mapPartitions(lambda x: count_gbdt(x, gbdt))
    feature_df = feature_rdd.toDF().withColumnRenamed(
        '_1', 'bbd_qyxx_id'
    ).withColumnRenamed(
        '_2', 'company_name'
    ).withColumnRenamed(
        '_3', 'Score_GBDT'
    ).orderBy('bbd_qyxx_id')
    return feature_df

def change_type(row):
    row_dict = dict(
        bbd_qyxx_id=row[0],
        company_name=row[1],
        total_score=row[2],
        GM_company_strength_risk=row[3],
        GM_behavior_risk=row[4],
        GM_credit_risk=row[5],
        GM_static_relationship_risk=row[6],
        GM_dynamic_relationship_risk=row[7]
    )
    return json.dumps(row_dict, ensure_ascii=False)

def run():
    prd_logistic_df = logistic_score()
    # writeCSV(prd_logistic_df, '/user/antifraud/hongjing5/test/', 'prd_logistic')
    # prd_logistic_df.show()
    prd_gbdt_df = GBDT_score()
    # writeCSV(prd_gbdt_df, '/user/antifraud/hongjing5/test/', 'prd_gbdt')
    # prd_gbdt_df.show()

    prd_logistic_df.registerTempTable('L')
    prd_gbdt_df.registerTempTable('R')

    sqlstr = '''
    select L.bbd_qyxx_id, L.company_name, (Score_GBDT*0.885+Score_logistic*0.862)/(0.885+0.862) Score_logistic_GBDT, GM_company_strength_risk, GM_behavior_risk, GM_credit_risk, GM_static_relationship_risk, GM_dynamic_relationship_risk from L 
    Left join R on L.bbd_qyxx_id == R.bbd_qyxx_id
    order by bbd_qyxx_id
    '''
    prd_result_df = runSQL(sqlstr)
    # writeCSV(prd_result_df, '/user/antifraud/hongjing5/test/', 'prd_result')
    #     hadoop fs -getmerge /user/antifraud/hongjing5/test/prd_logistic /data5/antifraud/qiling/data/prd_logistic.csv
    #     hadoop fs -getmerge /user/antifraud/hongjing5/test/prd_gbdt /data5/antifraud/qiling/data/prd_gbdt.csv
    #     hadoop fs -getmerge /user/antifraud/hongjing5/test/prd_result /data5/antifraud/qiling/data/prd_result.csv
    prd_result_rdd = prd_result_df.rdd.map(
        change_type
    )
    # print prd_result_rdd.first()

    os.system(
        ("hadoop fs -rmr "
         "{path}/"
         "nf_feature_risk_score/{version}").format(path=OUT_PATH,
                                                   version=RELATION_VERSION))

    codec = "org.apache.hadoop.io.compress.GzipCodec"
    prd_result_rdd.coalesce(
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
        .appName("hgongjing5_two_prd_nf_risk_score") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

if __name__ == '__main__':
    conf = configparser.ConfigParser()
    conf.read("/data5/antifraud/qiling/conf/hongjing5.ini", encoding='UTF-8')
    MODEL_FILE = '/data5/antifraud/qiling/data/gbdt_2018_02_04.pkl'
    
    IN_PATH = conf.get('feature_preprocessing', 'OUT_PATH')
    IN_PATH_GBDT = conf.get('feature_merge', 'OUT_PATH')
    OUT_PATH = conf.get('risk_score', 'OUT_PATH')
    WEIGHT_PATH = conf.get('risk_score', 'WEIGHT_PATH')
    
    # 中间结果版本

    RELATION_VERSION = '20171219'

    spark = get_spark_session()

    # 指标的维度
    DIMENSION = 61
    # 逻辑回归迭代次数
    ITERATION = 10

    run()
