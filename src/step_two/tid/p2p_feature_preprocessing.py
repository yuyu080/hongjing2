# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
p2p_feature_preprocessing.py {version}
'''

import os
import sys

import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StandardScaler

def get_vectors(row):
    if row['feature_26']:
        return (Vectors.dense([
                row['p2p_feature_1'],
                row['p2p_feature_2'],
                row['p2p_feature_3'],
                row['p2p_feature_4'],
                row['p2p_feature_5'],
                row['p2p_feature_6'],
                row['p2p_feature_7'],
                row['p2p_feature_8'],
                row['p2p_feature_9'],
                row['p2p_feature_10'],
                row['p2p_feature_11'],
                row['p2p_feature_12'],
                row['p2p_feature_13'],
                row['p2p_feature_14'],
                row['p2p_feature_15'],
                row['p2p_feature_16'],
                row['feature_1']['r'],
                row['feature_2']['c'],
                row['feature_3']['z'],
                row['feature_4']['k'],
                row['feature_5']['l'],
                row['feature_6']['z'],
                row['feature_7']['z'],
                row['feature_8']['y'],
                row['feature_9']['n'],
                row['feature_10']['z'],
                row['feature_11']['z'],
                row['feature_12']['z'],
                row['feature_13']['z'],
                row['feature_14']['z'],
                row['feature_15']['r'],
                row['feature_16']['r'],
                row['feature_17']['r'],
                row['feature_18']['z'],
                row['feature_19']['z'],
                row['feature_20']['g'],
                row['feature_21']['y'],
                row['feature_22']['z'],
                row['feature_23']['y'],
                row['feature_24']['z'],
                row['feature_26']['a_1'],
                row['feature_26']['a_4'],
                row['feature_26']['a_5'],
                row['feature_26']['a_6'],
                row['feature_26']['b_1'],
                row['feature_26']['b_2'],
                row['feature_26']['b_3'],
                row['feature_26']['c_1'],
                row['feature_26']['d_2'],
            ]),
            row['bbd_qyxx_id'],
            row['company_name'],
            row['platform_name'],
            row['platform_state'])
    elif row['feature_1']:
        return (Vectors.dense([
                row['p2p_feature_1'],
                row['p2p_feature_2'],
                row['p2p_feature_3'],
                row['p2p_feature_4'],
                row['p2p_feature_5'],
                row['p2p_feature_6'],
                row['p2p_feature_7'],
                row['p2p_feature_8'],
                row['p2p_feature_9'],
                row['p2p_feature_10'],
                row['p2p_feature_11'],
                row['p2p_feature_12'],
                row['p2p_feature_13'],
                row['p2p_feature_14'],
                row['p2p_feature_15'],
                row['p2p_feature_16'],
                row['feature_1']['r'],
                row['feature_2']['c'],
                row['feature_3']['z'],
                row['feature_4']['k'],
                row['feature_5']['l'],
                row['feature_6']['z'],
                row['feature_7']['z'],
                row['feature_8']['y'],
                row['feature_9']['n'],
                row['feature_10']['z'],
                row['feature_11']['z'],
                row['feature_12']['z'],
                row['feature_13']['z'],
                row['feature_14']['z'],
                row['feature_15']['r'],
                row['feature_16']['r'],
                row['feature_17']['r'],
                row['feature_18']['z'],
                row['feature_19']['z'],
                row['feature_20']['g'],
                row['feature_21']['y'],
                row['feature_22']['z'],
                row['feature_23']['y'],
                row['feature_24']['z'],
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
            ]), 
            row['bbd_qyxx_id'],
            row['company_name'],
            row['platform_name'],
            row['platform_state'])
    else:
        return (Vectors.dense([
                row['p2p_feature_1'],
                row['p2p_feature_2'],
                row['p2p_feature_3'],
                row['p2p_feature_4'],
                row['p2p_feature_5'],
                row['p2p_feature_6'],
                row['p2p_feature_7'],
                row['p2p_feature_8'],
                row['p2p_feature_9'],
                row['p2p_feature_10'],
                row['p2p_feature_11'],
                row['p2p_feature_12'],
                row['p2p_feature_13'],
                row['p2p_feature_14'],
                row['p2p_feature_15'],
                row['p2p_feature_16'],
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
                0.,
            ]), 
            row['bbd_qyxx_id'],
            row['company_name'],
            row['platform_name'],
            row['platform_state'])

def spark_data_flow():
    input_df = spark.read.parquet(
        ("{path}/"
         "p2p_feature_merge/"
         "{version}").format(path=IN_PAHT, 
                             version=RELATION_VERSION))

    tid_vector_df = input_df.rdd.map(
        get_vectors
    ).toDF(
    ).withColumnRenamed(
        '_1', 'features'
    ).withColumnRenamed(
        '_2', 'bbd_qyxx_id'
    ).withColumnRenamed(
        '_3', 'company_name'
    ).withColumnRenamed(
        '_4', 'platform_name'
    ).withColumnRenamed(
        '_5', 'platform_state'
    )
    
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                            withStd=True, withMean=True)
    
    # Compute summary statistics by fitting the StandardScaler
    scalerModel = scaler.fit(tid_vector_df)
    
    # Normalize each feature to have unit standard deviation.
    scaled_df = scalerModel.transform(tid_vector_df)
    
    return scaled_df

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
        .appName("hgongjing2_two_tid_p2p_feature_preprocessing") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 

def run():
    tid_df = spark_data_flow()
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "p2p_feature_preprocessing/"
         "{version}").format(path=OUT_PATH, 
                             version=RELATION_VERSION)) 
    tid_df.repartition(10).write.parquet(      
        ("{path}/"
         "p2p_feature_preprocessing/"
         "{version}").format(path=OUT_PATH, 
                             version=RELATION_VERSION))
    
if __name__ == '__main__':
    conf = configparser.ConfigParser()
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    #输入参数
    RELATION_VERSION = sys.argv[1]
    
    IN_PAHT = conf.get('feature_merge', 'OUT_PATH')
    OUT_PATH = conf.get('feature_preprocessing', 'OUT_PATH')
    
    spark = get_spark_session()
    
    run()