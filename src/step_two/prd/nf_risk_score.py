# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
nf_risk_score.py
'''

import os
import sys
import json

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def get_label_probability(iter_data):
    '''
    调用通用模型
    '''
    import xgboost as xgb
    
    company_names = []
    bbd_qyxx_ids = []
    data_set = []
    
    for each_row in iter_data:
        if (each_row['feature_25'] and each_row['feature_26'] and 
                  each_row['feature_27'] and each_row['feature_28']):
            data = [each_row['feature_1']['r'], each_row['feature_2']['x'],
                    each_row['feature_3']['j'], each_row['feature_4']['k'],
                    each_row['feature_5']['l'], each_row['feature_6']['z'],
                    each_row['feature_7']['z'], each_row['feature_8']['t_1'],
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
        else:
            data = [each_row['feature_1']['r'], each_row['feature_2']['x'],
                    each_row['feature_3']['j'], each_row['feature_4']['k'],
                    each_row['feature_5']['l'], each_row['feature_6']['z'],
                    each_row['feature_7']['z'], each_row['feature_8']['t_1'],
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
        company_names.append(each_row['company_name'])
        bbd_qyxx_ids.append(each_row['bbd_qyxx_id'])
        data_set.append(data)
   
    dpred = xgb.DMatrix(data_set)
    bst = xgb.Booster()
    bst.load_model("GM_release_28feats.model")
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
    根据权重分布对每个一级指标打分，GM为通用部门的权重
    '''
    from risk_weight import risk_weight

    gm_weight = risk_weight['GM']
    total_score = round(change_score(row[2]*100.), 1)
    risk_distribution = {k: round(v*total_score/100., 1) 
                         for k, v in gm_weight.iteritems()}
    row_dict = dict(
        bbd_qyxx_id = row[0],
        company_name = row[1],
        total_score = total_score,
        **risk_distribution
    )
    return json.dumps(row_dict, ensure_ascii=False)

def spark_data_flow(input_version):
    feature_df = spark.read.parquet(
    "{path}/nf_feature_merge/{version}".format(path=IN_PATH,
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
         "nf_feature_risk_score/{version}").format(path=OUT_PATH, 
                                                  version=RELATION_VERSION))    
    prd_df.repartition(10).saveAsTextFile(
        ("{path}/"
         "nf_feature_risk_score/{version}").format(path=OUT_PATH, 
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
        .appName("hgongjing2_two_prd_nf_risk_score") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark

if __name__ == '__main__':
    MODEL_FILE = ("/data5/antifraud/Hongjing2/data/inputdata/model/"
                  "GM_release_28feats.model")
    WEIGHT_FILE = ("/data5/antifraud/Hongjing2/data/inputdata/weight/"
                  "risk_weight.py")
    FILES = ','.join([MODEL_FILE, 
                      WEIGHT_FILE])
    
    IN_PATH = "/user/antifraud/hongjing2/dataflow/step_two/raw"
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_two/prd"
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    spark = get_spark_session()
    
    run()