# -*- encoding=utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
nf_feature_preprocessing.py {version}
'''

import os

import sys

import configparser
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StandardScaler

reload(sys)
sys.setdefaultencoding("utf-8")


def get_vectors(row):
    return (Vectors.dense([
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            row[10],
            row[11],
            row[12],
            row[13],
            row[14],
            row[15],
            row[16],
            row[17],
            row[18],
            row[19],
            row[20],
            row[21],
            row[22],
            row[23],
            row[24],
            row[25],
            row[26],
            row[27],
            row[28],
            row[29],
            row[30],
            row[31],
            row[32],
            row[33],
            row[34],
            row[35],
            row[36],
            row[37],
            row[38],
            row[39],
            row[40],
            row[41],
            row[42],
            row[43],
            row[44],
            row[45],
            row[46],
            row[47],
            row[48],
            row[49],
            row[50],
            row[51],
            row[52],
            row[53],
            row[54],
            row[55],
            row[56],
            row[57],
            row[58],
            row[59],
            row[60],
    ]),
            row[61],
            row[62])

def get_datas(row):
    return (
        row['feature_1']['r_i'],
        row['feature_1']['x'],
        row['feature_10']['0']['lending_1'],
        row['feature_10']['0']['rmfygg_1'],
        row['feature_10']['0']['zgcpwsw'],
        row['feature_10']['0']['zgcpwsw_1'],
        row['feature_10']['0']['zgcpwsw_specific'],
        row['feature_10']['0']['zgcpwsw_specific_1'],
        row['feature_10']['1']['lending_1'],
        row['feature_10']['1']['rmfygg_1'],
        row['feature_10']['2']['ktgg_1'],
        row['feature_10']['2']['lending'],
        row['feature_10']['2']['lending_1'],
        row['feature_10']['2']['zgcpwsw_specific'],
        row['feature_10']['2']['zgcpwsw_specific_1'],
        row['feature_12']['0']['dishonesty'],
        row['feature_12']['0']['zhixing'],
        row['feature_12']['0']['zhixing_1'],
        row['feature_12']['1']['dishonesty'],
        row['feature_12']['1']['dishonesty_1'],
        row['feature_12']['2']['dishonesty_1'],
        row['feature_13']['0']['jyyc'],
        row['feature_13']['0']['jyyc_1'],
        row['feature_13']['2']['jyyc'],
        row['feature_13']['2']['jyyc_1'],
        row['feature_15']['r'],
        row['feature_15']['x_1'],
        row['feature_15']['y_1'],
        row['feature_15']['y_2'],
        row['feature_19']['x_2'],
        row['feature_19']['y_2'],
        row['feature_19']['y_3'],
        row['feature_2']['c'],
        row['feature_2']['x'],
        row['feature_2']['y'],
        row['feature_20']['y_1'],
        row['feature_22']['d'],
        row['feature_23']['b_1'],
        row['feature_23']['b_2'],
        row['feature_23']['c_1'],
        row['feature_23']['d_1'],
        row['feature_23']['d_3'],
        row['feature_24']['z'],
        row['feature_5']['c_i'],
        row['feature_5']['l'],
        row['feature_6']['c_1'],
        row['feature_6']['c_2'],
        row['feature_6']['c_3'],
        row['feature_6']['c_5'],
        row['feature_6']['c_6'],
        row['feature_7']['e'],
        row['feature_7']['e_2'],
        row['feature_7']['e_4'],
        row['feature_7']['z'],
        row['feature_8']['t_1'],
        row['feature_8']['t_2'],
        row['feature_8']['t_3'],
        row['feature_26']['a_1'],
        row['feature_26']['a_5'],
        row['feature_26']['b_3'],
        row['feature_26']['c_1'],
        row['bbd_qyxx_id'],
        row['company_name'])

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
        .appName("hgongjing5_two_tid_nf_feature_preprocessing") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


def spark_data_flow():
    # 从merge里面读数据（这个数据是parquet的格式，很神奇）
    input_df = spark.read.parquet(
        ("{path}/"
         "nf_feature_merge/"
         "{version}").format(path=IN_PAHT, 
                             version=RELATION_VERSION))
    input_df = input_df.rdd.map(
        get_datas
    ).toDF().fillna(0.)
    tid_vector_df = input_df.rdd.map(
        get_vectors
    ).toDF(
    ).withColumnRenamed(
        '_1', 'features'
    ).withColumnRenamed(
        '_2', 'bbd_qyxx_id'
    ).withColumnRenamed(
        '_3', 'company_name'
    )

    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                            withStd=True, withMean=True)

    # Compute summary statistics by fitting the StandardScaler
    scalerModel = scaler.fit(tid_vector_df)

    # Normalize each feature to have unit standard deviation.
    scaled_df = scalerModel.transform(tid_vector_df)

    return scaled_df

def run():
    tid_df = spark_data_flow()
    # print "gql"
    # tid_df.show()
    # 保存之后就结束鸟
    os.system(
        ("hadoop fs -rmr "
         "{path}/"
         "nf_feature_preprocessing/"
         "{version}").format(path=OUT_PATH, version=RELATION_VERSION))
    tid_df.repartition(10).write.parquet(
        ("{path}/"
         "nf_feature_preprocessing/"
         "{version}").format(path=OUT_PATH, version=RELATION_VERSION))

if __name__ == '__main__':
    # 读配置文件
    conf = configparser.ConfigParser()
    conf.read("/data5/antifraud/qiling/conf/hongjing5.ini", encoding='UTF-8')

    # 输入参数，哎
    RELATION_VERSION = '20171219'

    # 上一步merge作为输入
    IN_PAHT = conf.get('feature_merge', 'OUT_PATH')

    # 这一步preprocessing作为输出
    OUT_PATH = conf.get('feature_preprocessing', 'OUT_PATH')

    spark = get_spark_session()


    run()