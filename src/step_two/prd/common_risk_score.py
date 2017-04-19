# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
common_risk_score.py
'''

import os
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
            data = [each_row['feature_1']['r'], each_row['feature_2']['c'],
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
        else:
            data = [each_row['feature_1']['r'], each_row['feature_2']['c'],
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
        company_names.append(each_row['company_name'])
        bbd_qyxx_ids.append(each_row['bbd_qyxx_id'])
        data_set.append(data)
   
    dpred = xgb.DMatrix(data_set)
    bst = xgb.Booster()
    bst.load_model("Model_for_28feats_release.model")
    ypred  = bst.predict(dpred)

    return zip(bbd_qyxx_ids, company_names, ypred)

def get_json_obj(row):
    '''
    将数据序列化
    '''
    row_dict = dict(
        bbd_qyxx_id = row[0],
        company_name = row[1],
        risk_score = round(row[2]*100, 2)    
    )
    
    return json.dumps(row_dict, ensure_ascii=False)

def spark_data_flow(input_version):
    feature_df = spark.read.parquet(
    "{path}/common_feature_merge/{version}".format(path=IN_PATH,
                                                   version=input_version))

    feature_rdd = feature_df.rdd \
        .coalesce(100) \
        .mapPartitions(get_label_probability) \
        .map(get_json_obj)
    
    return feature_rdd

def run():
    prd_df = spark_data_flow(RAW_VERSION)
    
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "common_feature_risk_score/{version}").format(path=OUT_PATH, 
                                                  version=PRD_VERSION))    
    prd_df.repartition(10).saveAsTextFile(
        ("{path}/"
         "common_feature_risk_score/{version}").format(path=OUT_PATH, 
                                                  version=PRD_VERSION))

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
        .appName("hgongjing2_two_prd_common_risk_score") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark

if __name__ == '__main__':
    MODEL_FILE = ("/data5/antifraud/Hongjing2.0/data/inputdata/models/"
                  "Model_for_28feats_release.model")
    IN_PATH = "/user/antifraud/hongjing2/dataflow/step_two/raw"
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_two/prd"
    PRD_VERSION = TID_VERSION = RAW_VERSION =  "20170403"
    
    spark = get_spark_session()
    
    run()