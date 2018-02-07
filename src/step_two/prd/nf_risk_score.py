# -*- coding: utf-8 -*-
# 重新写提交命令吧
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
nf_risk_score.py {version}
修改：重新实现主体逻辑
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
    '''
    计算线性y值函数，被其他函数调用
    :param data: 要计算的数据
    :param weight: 权重
    :return: 计算结果
    '''
    return np.dot(data, weight[1:]) + weight[0]

def sigmoid(x):
    '''
    sigmoid函数，被其他函数调用
    :param x: 要计算的数据
    :return: 计算结果
    '''
    return 1. / (1 + math.exp(-x))

def score_change(data, index):
    '''
    指数幂计算函数，被其他函数调用
    :param data: 要计算的数据
    :param index: 幂值
    :return: 计算结果
    '''
    return math.pow(data, index) * 100

def logistic_count(data, weight, index):
    '''
    logistics计算
    :param data: 要计算的数据
    :param weight: 权重数据
    :param index: 幂值
    :return: 计算结果
    '''
    return score_change(sigmoid(linear(data, weight)), index)

def logistic_count_sub(data, weight, b, index):
    '''
    对风险子模块进行计算
    :param data: 要计算的子模块数据
    :param weight: 权重数据
    :param b: 截距项
    :param index: 幂值
    :return: 计算结果
    '''
    return score_change(sigmoid(np.dot(data, weight) + b/DIMENSION * len(weight)), index)

def runSQL(sqlstr):
    '''
    执行sql语句
    :param sqlstr: 要执行的sql语句
    :return: dataframe数据，并清洗null值数据
    '''
    return spark.sql(sqlstr).na.replace(['null', 'NULL', 'NaN', 'nan'], ['', '', '', '']).fillna(0.)

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
    '''
    计算logistics子模快
    :param iter_data: 每个分区的数据
    :param weight: 权重
    :param zhibiao: 子模块指标值
    :param sk: sk值
    :param b: 截距项
    :param regression_coefficient: 线性回归系数
    :return: rdd形式的数据
    '''
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
    '''
    计算logistics主模快
    :return: dataframe形式的结果数据
    '''
    # 读logistics原数据
    input_df = spark.read.parquet(
        "{path}/nf_feature_preprocessing/"
        "{version}".format(path=IN_PATH, version=RELATION_VERSION))
    # 读权重数据weight
    weight_df = spark.read.csv(WEIGHT_PATH, encoding="utf-8", header=True, sep='\t')
    weight_df.registerTempTable('weight')
    weight_df = spark.sql('''
    select cast(V1 as double) V1, cast(V2 as double) V2, cast(V3 as double) V3, cast(V4 as double) V4, cast(V5 as double) V5, cast(V6 as double) V6, cast(V7 as double) V7, cast(V8 as double) V8, cast(V9 as double) V9, cast(V10 as double) V10 from weight
    ''')
    # 得到权重数据列表
    weight = weight_df.rdd.collect()
    # 计算每一行的均值，为平均回归系数(包括截距项，要加1)
    regression_coefficient = []
    for i in range(0, DIMENSION + 1, 1):
        regression_coefficient.append(sum(weight[i])/ITERATION)
    print regression_coefficient
    # 对权重数据进行转置处理
    weight = facotrMatrix(weight)
    # weight[0]表示第一列，注意包括了截距项，因此做乘法的时候要去掉截距项再加上截距项

    # 风险各子模块需要取的数据，写到一个总的列表下面
    zhibiao = [[1, 2, 33, 34, 35], [44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57], range(3, 26), [26, 27, 28, 29, 30, 31, 32, 36, 37, 38, 39, 40, 41, 42, 43], range(58, 62)]
    # sk分
    sk = [0.8, 0.5, 0.4, 0.5, 0.8]
    # 系数
    b = regression_coefficient[0]

    # 执行计算，输出logistics得分和各风险子模块得分
    raw_rdd = input_df.rdd.mapPartitions(lambda x: count_logistic(x, weight, zhibiao, sk, b, regression_coefficient))
    # 转成df并重新命名
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

    return raw_df

def count_gbdt(iter_data, gbdt):
    '''
    gbdt计算子模块
    :param iter_data: 每个分区的数据
    :param gbdt: gbdt模型数据
    :return: rdd结果数据
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
    prob = gbdt.predict_proba(pd.DataFrame(data_set))
    prob = facotrMatrix(prob)[1]
    gbdtS = []
    for list in prob:
        gbdtS.append(score_change(list, 0.2))

    return zip(bbd_qyxx_ids, company_names, gbdtS)


def GBDT_score():
    '''
    gbdt主模块
    :return: dataframe形式的结果数据
    '''
    # 读gbdt原数据
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
    '''
    由于结果数据是dataframe格式，将其改为标准输出形式
    :param row: 每一行数据
    :return: 标准输出格式数据
    '''
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
    '''
    执行主模块
    :return: 无
    '''
    # logistics结果数据
    prd_logistic_df = logistic_score()

    # gbdt结果数据
    prd_gbdt_df = GBDT_score()

    prd_logistic_df.registerTempTable('L')
    prd_gbdt_df.registerTempTable('R')

    sqlstr = '''
    select L.bbd_qyxx_id, L.company_name, (Score_GBDT*0.885+Score_logistic*0.862)/(0.885+0.862) Score_logistic_GBDT, GM_company_strength_risk, GM_behavior_risk, GM_credit_risk, GM_static_relationship_risk, GM_dynamic_relationship_risk from L 
    Left join R on L.bbd_qyxx_id == R.bbd_qyxx_id
    order by bbd_qyxx_id
    '''
    # 执行sql语句，得到最终结果
    prd_result_df = runSQL(sqlstr)

    # 将最终结果转换成标准数据输出格式
    prd_result_rdd = prd_result_df.rdd.map(
        change_type
    )

    # 保存数据
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
    # 上传，并更改了模型C6路径
    MODEL_FILE = '/data5/antifraud/qiling/data/gbdt_2018_02_04.pkl'

    IN_PATH = conf.get('feature_preprocessing', 'OUT_PATH')
    # 增加了gbdt数据读取路径
    IN_PATH_GBDT = conf.get('feature_merge', 'OUT_PATH')
    OUT_PATH = conf.get('risk_score', 'OUT_PATH')
    # 增加了权重路径
    WEIGHT_PATH = conf.get('risk_score', 'WEIGHT_PATH')

    RELATION_VERSION = sys.argv[1]

    spark = get_spark_session()

    # 指标的维度
    DIMENSION = 61
    # 逻辑回归迭代次数
    ITERATION = 10

    run()
