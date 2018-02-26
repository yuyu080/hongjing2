# -*- encoding=utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
nf_feature_merge.py {version}

修改内容：保存的特征，包括logistic和gdbt两块特征
'''
import os

import configparser
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
reload(sys)
sys.setdefaultencoding("utf-8")
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
        .appName("hgongjing5_two_raw_nf_feature_merge") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def spark_data_flow(static_version, dynamic_version):
    '''
    合并静态与动态风险：当动态风险不存时，值为null，类型为srtuct
    '''
    static_df = spark.read.json(
        "{path}/"
        "common_static_feature_distribution_v2/"
        "{version}".format(path=IN_PATH_TWO,
                           version=static_version))
    dynamic_df = spark.read.json(
        "{path}/"
        "common_dynamic_feature_distribution_v2/"
        "{version}".format(path=IN_PATH_TWO,
                           version=dynamic_version))

    sample_df = spark.read.parquet(
        ("{path}"
         "/ljr_sample/"
         "{version}").format(path=IN_PATH_ONE,
                             version=RELATION_VERSION))

    # 我修改了需要取出的特征，包括logistic和gdbt两块特征
    some_type_df = sample_df.where(
        sample_df.company_type.isin(TYPE_NF_LIST)
    )

    feature_df = static_df.join(
        dynamic_df,
        static_df.bbd_qyxx_id == dynamic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        some_type_df,
        some_type_df.bbd_qyxx_id == static_df.bbd_qyxx_id
    ).select(
        [static_df.bbd_qyxx_id,
         static_df.company_name,
         'feature_1', 'feature_10', 'feature_12', 'feature_13', 'feature_15', 'feature_16', 'feature_17', 'feature_18', 'feature_19', 'feature_2', 'feature_20', 'feature_22', 'feature_23', 'feature_24', 'feature_26', 'feature_4', 'feature_5', 'feature_6', 'feature_7', 'feature_8', 'feature_9']
    )
    return feature_df

def run():

    raw_df = spark_data_flow(static_version=RELATION_VERSION,
                             dynamic_version=RELATION_VERSION)

    os.system(
        ("hadoop fs -rmr "
         "{path}/"
         "nf_feature_merge/{version}").format(path=OUT_PATH,
                                              version=RELATION_VERSION))

    raw_df.repartition(10).write.parquet(
        ("{path}/"
         "nf_feature_merge/{version}").format(path=OUT_PATH,
                                              version=RELATION_VERSION))

if __name__ == '__main__':
    # 读取配置文件，这块暂时存到我的文件目录下面
    config = configparser.ConfigParser()
    config.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")

    TYPE_NF_LIST = eval(config.get('input_sample_data', 'TYPE_NF_LIST'))

    RELATION_VERSION = sys.argv[1]

    IN_PATH_ONE = config.get('input_sample_data', 'OUT_PATH')
    IN_PATH_TWO = config.get('common_company_feature', 'OUT_PATH')
    OUT_PATH = config.get('feature_merge', 'OUT_PATH')

    spark = get_spark_session()

    run()